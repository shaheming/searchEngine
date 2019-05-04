package edu.uci.ics.cs221.index.inverted;
//todo
import com.google.common.base.Preconditions;
import edu.uci.ics.cs221.analysis.Analyzer;
import edu.uci.ics.cs221.storage.Document;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

class SegmentEntry {
  private String name;
  private Integer headerLen;

  public String getName() {
    return name;
  }

  public Integer getHeaderLen() {
    return headerLen;
  }

  public SegmentEntry(String name, Integer headerLen) {
    this.name = name;
    this.headerLen = headerLen;
  }
}

/**
 * This class manages an disk-based inverted index and all the documents in the inverted index.
 *
 * <p>Please refer to the project 2 wiki page for implementation guidelines.
 */
public class InvertedIndexManager {

  /**
   * The default flush threshold, in terms of number of documents. For example, a new Segment should
   * be automatically created whenever there's 1000 documents in the buffer.
   *
   * <p>In test cases, the default flush threshold could possibly be set to any number.
   */
  public static int DEFAULT_FLUSH_THRESHOLD = 1000;
  /**
   * The default merge threshold, in terms of number of segments in the inverted index. When the
   * number of segments reaches the threshold, a merge should be automatically triggered.
   *
   * <p>In test cases, the default merge threshold could possibly be set to any number.
   */
  public static int DEFAULT_MERGE_THRESHOLD = 8;

  /**
   * Key => segmentName, val => segmentHeaderSize segmentMetaData.txt: segments X No., fileName,
   * fileHeaderSize 0 xxxx xxxx
   */
  private Analyzer analyzer;

  private InvertedIndex currInvertIndex;
  private final Map<Integer, SegmentEntry> segmentMetaData =
      Collections.synchronizedMap(new TreeMap<>());
  private String workPath;

  // todo 4. key the metadate in memory used to do search
  // todo 5. the invert list is sorted by time and use a indexArray to map No. seg to seg name then
  // to the segmentMetaData
  private InvertedIndexManager(String indexFolder, Analyzer analyzer) {
    this.analyzer = analyzer;
    this.currInvertIndex = new InvertedIndex(indexFolder);
    this.workPath = indexFolder;
  }

  public static void loadMetaData(InvertedIndexManager inv, Path filePath) {
    try {
      List<String> lines = Files.readAllLines(filePath);
      for (String line : lines.subList(1, lines.size())) {
        String[] cols = line.split("\\s");
        inv.segmentMetaData.put(
            Integer.valueOf(cols[0]), new SegmentEntry(cols[1], Integer.valueOf(cols[2])));
        System.out.println("read segment: " + cols[0] + " " + cols[1] + " info");
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public static InvertedIndexManager open(String indexFolder, Analyzer analyzer) {
    InvertedIndexManager inv = new InvertedIndexManager(indexFolder, analyzer);
    loadMetaData(inv, Paths.get(indexFolder + "/metadata.txt"));
    return inv;
  }

  private void writeIndexMetaData() {
    try {
      BufferedWriter writer = new BufferedWriter(new FileWriter(this.workPath + "/metadata.txt"));

      synchronized (this.segmentMetaData) {
        writer.write(this.segmentMetaData.size() + "\n");
        for (Map.Entry<Integer, SegmentEntry> entry : this.segmentMetaData.entrySet()) {
          writer.write(
              entry.getKey()
                  + " "
                  + entry.getValue().getName()
                  + " "
                  + entry.getValue().getHeaderLen()
                  + "\n");
        }
      }
      writer.close();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /** Creates an inverted index manager with the folder and an analyzer */
  public static InvertedIndexManager createOrOpen(String indexFolder, Analyzer analyzer) {
    try {
      Path indexFolderPath = Paths.get(indexFolder);
      if (Files.exists(indexFolderPath) && Files.isDirectory(indexFolderPath)) {
        if (Files.isDirectory(indexFolderPath)) {
          return new InvertedIndexManager(indexFolder, analyzer);
        } else {
          throw new RuntimeException(indexFolderPath + " already exists and is not a directory");
        }
      } else {
        Files.createDirectories(indexFolderPath);
        return new InvertedIndexManager(indexFolder, analyzer);
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * Adds a document to the inverted index. Document should live in a in-memory buffer until
   * `flush()` is called to write the segment to disk.
   *
   * @param document
   */
  public void addDocument(Document document) {
    this.currInvertIndex.addDocument(
        document, new HashSet<String>(this.analyzer.analyze(document.getText())));
    if (this.currInvertIndex.getDocNum() >= DEFAULT_FLUSH_THRESHOLD) this.flush();
  }

  /**
   * Flushes all the documents in the in-memory segment buffer to disk. If the buffer is empty, it
   * should not do anything. flush() writes the segment to disk containing the posting list and the
   * corresponding document store. calculate the metadate of the of the invertList create a new
   * invertList todo when we call this method we can create a thread to flush index the main thread
   * can continue to add document
   */
  public void flush() {
    if (this.currInvertIndex.getDocNum() == 0) return;
    InvertedIndex oldInvertList = this.currInvertIndex;
    oldInvertList.flush();

    this.segmentMetaData.put(
        this.segmentMetaData.size(),
        new SegmentEntry(
            this.currInvertIndex.getSegmentName(), this.currInvertIndex.getHeaderLen()));
    this.writeIndexMetaData();
    this.currInvertIndex = new InvertedIndex(oldInvertList.getBasePath());

    if (this.segmentMetaData.size() >= DEFAULT_MERGE_THRESHOLD) {
      //      Runnable task =
      //          () -> {
      //            this.mergeAllSegments();
      //          };
      //      task.run();
      this.mergeAllSegments();
    }
  }

  static class ParallelMerge extends Thread {
    String put;
    InvertedIndex inv1;
    InvertedIndex inv2;
    final Map<Integer, SegmentEntry> metaData;
    Integer desSegId;

    public ParallelMerge(
        String name,
        String path,
        final Map<Integer, SegmentEntry> metaData,
        SegmentEntry s1,
        SegmentEntry s2,
        Integer desSegId) {
      super(name);
      this.put = s1.getName() + " " + s2.getName();
      this.inv1 = InvertedIndex.openInvertList(path, s1.getName(), s1.getHeaderLen());
      this.inv2 = InvertedIndex.openInvertList(path, s2.getName(), s2.getHeaderLen());
      this.desSegId = desSegId;
      this.metaData = metaData;
    }

    public void run() {
      try {
        InvertedIndex inv = inv1.merge(inv2);
        SegmentEntry entry = new SegmentEntry(inv.getSegmentName(), inv.getHeaderLen());

        synchronized (this.metaData) {
          this.metaData.put(this.desSegId, entry);
        }
      } catch (RuntimeException e) {
        e.printStackTrace();
      }
      System.out.println(
          "Merge: " + this.put + ",thread name is ：" + Thread.currentThread().getName());
    }
  }

  public void mergeAllSegments() {
    // merge only happens at even number of segments
    Preconditions.checkArgument(getNumSegments() % 2 == 0);

    Map<Integer, SegmentEntry> synchronizedMap = Collections.synchronizedMap(new TreeMap<>());
    // do some thing
    int desSegId = 0;
    ExecutorService exec = Executors.newFixedThreadPool(4);
    synchronized (this.segmentMetaData) {
      Iterator<Map.Entry<Integer, SegmentEntry>> it = this.segmentMetaData.entrySet().iterator();
      while (it.hasNext()) {
        exec.execute(
            new ParallelMerge(
                "Thread: " + desSegId,
                this.workPath,
                synchronizedMap,
                it.next().getValue(),
                it.next().getValue(),
                desSegId));
        desSegId++;
      }
    }
    exec.shutdown();
    try {
      while (!exec.awaitTermination(1L, TimeUnit.HOURS)) {
        System.out.println("Not yet. Still waiting for termination");
      }

    } catch (InterruptedException e) {
    }

    System.out.println("Join merge");
    synchronized (this.segmentMetaData) {
      this.segmentMetaData.clear();
      this.segmentMetaData.putAll(synchronizedMap);
    }
  }

  /**
   * Performs a single keyword search on the inverted index. You could assume the analyzer won't
   * convert the keyword into multiple tokens. If the keyword is empty, it should not return
   * anything.
   *
   * @param keyword keyword, cannot be null.
   * @return a iterator of documents matching the query
   */
  public Iterator<Document> searchQuery(String keyword) {
    Preconditions.checkNotNull(keyword);
    ArrayList<String> keywords = new ArrayList<>();
    keywords.add(keyword);
    return parallelSearchQuery(keywords, "AND");
  }

  /**
   * Performs an AND boolean search on the inverted index.
   *
   * @param keywords a list of keywords in the AND query
   * @return a iterator of documents matching the query
   */
  public Iterator<Document> searchAndQuery(List<String> keywords) {
    return parallelSearchQuery(keywords, "AND");
  }

  private Iterator<Document> parallelSearchQuery(List<String> keywords, String searchMethod) {
    Preconditions.checkNotNull(keywords);
    Set<String> wordSet = new HashSet<>(keywords); // remove duplicated

    String words = String.join(" ", new LinkedList<String>(wordSet));
    ArrayList<String> token = new ArrayList<>(this.analyzer.analyze(words));
    ArrayList<ArrayList<SegmentEntry>> searchGroups = new ArrayList<>();
    synchronized (this.segmentMetaData) {
      SegmentEntry[] segs =
          this.segmentMetaData.values().toArray(new SegmentEntry[this.segmentMetaData.size()]);
      int threshold = this.segmentMetaData.size() / 2;
      searchGroups.add(
          new ArrayList<SegmentEntry>(Arrays.asList(Arrays.copyOfRange(segs, 0, threshold))));
      searchGroups.add(
          new ArrayList<SegmentEntry>(
              Arrays.asList(Arrays.copyOfRange(segs, threshold, segs.length))));
    }
    ExecutorService exec = Executors.newFixedThreadPool(4);
    Map<String, Document> synchronizedMap = Collections.synchronizedMap(new TreeMap<>());
    for (ArrayList<SegmentEntry> group : searchGroups) {
      Runnable runnableTask =
          () -> {
            for (SegmentEntry entry : group) {
              Map<String, Document> dos =
                  InvertedIndex.openInvertList(this.workPath, entry.getName(), entry.getHeaderLen())
                      .searchQuery(token, searchMethod);
              synchronized (synchronizedMap) {
                synchronizedMap.putAll(dos);
              }
            }
          };
      exec.execute(runnableTask);
    }
    exec.shutdown();
    try {
      while (!exec.awaitTermination(1L, TimeUnit.HOURS)) {
        System.out.println("Not yet. Still waiting for termination");
      }

    } catch (InterruptedException e) {

    }
    ArrayList<Document> res = new ArrayList<>(synchronizedMap.values());
    return res.iterator();
  }

  /**
   * Performs an OR boolean search on the inverted index.
   *
   * @param keywords a list of keywords in the OR query
   * @return a iterator of documents matching the query
   */
  public Iterator<Document> searchOrQuery(List<String> keywords) {
    Preconditions.checkNotNull(keywords);

    return parallelSearchQuery(keywords, "OR");
  }

  /** Iterates through all the documents in all disk segments. */
  public Iterator<Document> documentIterator() {
    loadMetaData(this, Paths.get(this.workPath + "/metadata.txt"));
    ArrayList<Document> docs = new ArrayList<>();
    for (SegmentEntry entry : this.segmentMetaData.values()) {
      InvertedIndex inv =
          InvertedIndex.openInvertList(this.workPath, entry.getName(), entry.getHeaderLen());
      docs.addAll(inv.getAllDocuments().values());
    }
    return docs.iterator();
  }

  /**
   * Deletes all documents in all disk segments of the inverted index that match the query.
   *
   * @param keyword
   */
  public void deleteDocuments(String keyword) {
    throw new UnsupportedOperationException();
  }

  /**
   * Gets the total number of segments in the inverted index. This function is used for checking
   * correctness in test cases.
   *
   * @return number of index segments.
   */
  public int getNumSegments() {
    int num;
    synchronized (this.segmentMetaData) {
      num = this.segmentMetaData.size();
    }
    return num;
  }

  /**
   * Reads a disk segment into memory based on segmentNum. This function is mainly used for checking
   * correctness in test cases.
   *
   * @param segmentNum n-th segment in the inverted index (start from 0).
   * @return in-memory data structure with all contents in the index segment, null if segmentNum
   *     don't exist.
   */
  public InvertedIndexSegmentForTest getIndexSegment(int segmentNum) {
    try {
      loadMetaData(this, Paths.get(this.workPath + "/metadata.txt"));
    } catch (UncheckedIOException e) {
      return null;
    }

    if (!this.segmentMetaData.containsKey(segmentNum)) return null;
    SegmentEntry entry = this.segmentMetaData.get(segmentNum);
    InvertedIndex inv =
        InvertedIndex.openInvertList(this.workPath, entry.getName(), entry.getHeaderLen());
    return new InvertedIndexSegmentForTest(inv.getAllInvertList(), inv.getAllDocuments());
  }
}
