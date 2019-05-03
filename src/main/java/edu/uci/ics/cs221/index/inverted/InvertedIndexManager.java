package edu.uci.ics.cs221.index.inverted;

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
  private final Set<SegmentEntry> segmentMetaData = Collections.synchronizedSet(new HashSet<>());
  private String workPath;

  // todo 4. key the metadate in memory used to do search
  // todo 5. the invert list is sorted by time and use a indexArray to map No. seg to seg name then
  // to the segmentMetaData
  private InvertedIndexManager(String indexFolder, Analyzer analyzer) {
    this.analyzer = analyzer;
    this.currInvertIndex = new InvertedIndex(indexFolder);
    this.workPath = indexFolder;
  }

  public static InvertedIndexManager open(String indexFolder, Analyzer analyzer) {
    try {
      List<String> lines = Files.readAllLines(Paths.get(indexFolder + "/metadata.txt"));
      InvertedIndexManager inv = new InvertedIndexManager(indexFolder, analyzer);
      for (String line : lines.subList(1, lines.size())) {
        String[] cols = line.split("\\s");
        inv.segmentMetaData.add(new SegmentEntry(cols[0], Integer.valueOf(cols[1])));
        System.out.println(cols[0] + " " + Integer.valueOf(cols[1]));
      }
      return inv;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private void writeIndexMetaData() {
    try {
      BufferedWriter writer = new BufferedWriter(new FileWriter(this.workPath + "/metadata.txt"));

      synchronized (this.segmentMetaData) {
        writer.write(this.segmentMetaData.size() + "\n");
        for (SegmentEntry entry : this.segmentMetaData) {
          writer.write(entry.getName() + " " + entry.getHeaderLen() + "\n");
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
    if (this.currInvertIndex.getDocNum() >= DEFAULT_FLUSH_THRESHOLD) this.flush();
    this.currInvertIndex.addDocument(
        document, new HashSet<String>(this.analyzer.analyze(document.getText())));
  }

  /**
   * Flushes all the documents in the in-memory segment buffer to disk. If the buffer is empty, it
   * should not do anything. flush() writes the segment to disk containing the posting list and the
   * corresponding document store. calculate the metadate of the of the invertList create a new
   * invertList todo when we call this method we can create a thread to flush index the main thread
   * can continue to add document
   */
  public void flush() {

    InvertedIndex oldInvertList = this.currInvertIndex;
    oldInvertList.flush();

    this.segmentMetaData.add(
        new SegmentEntry(
            this.currInvertIndex.getSegmentName(), this.currInvertIndex.getHeaderLen()));
    this.writeIndexMetaData();
    this.currInvertIndex = new InvertedIndex(oldInvertList.getBasePath());
  }

  static class ParallelMerge extends Thread {
    String put;
    InvertedIndex inv1;
    InvertedIndex inv2;
    Set<SegmentEntry> metaData;

    public ParallelMerge(
        String name,
        String path,
        final Set<SegmentEntry> metaData,
        SegmentEntry s1,
        SegmentEntry s2) {
      super(name);
      this.put = s1.getName() + " " + s2.getName();
      this.inv1 = InvertedIndex.openInvertList(path, s1.getName(), s1.getHeaderLen());
      this.inv2 = InvertedIndex.openInvertList(path, s2.getName(), s2.getHeaderLen());

      this.metaData = metaData;
    }

    public void run() {
      try {
        InvertedIndex inv = inv1.merge(inv2);
        SegmentEntry entry = new SegmentEntry(inv.getSegmentName(), inv.getHeaderLen());

        synchronized (this.metaData) {
          this.metaData.add(entry);
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

    Set<SegmentEntry> synchronizedMap = Collections.synchronizedSet(new HashSet<>());
    ArrayList<ParallelMerge> threads = new ArrayList<>();
    // do some thing
    int i = 0;
    synchronized (this.segmentMetaData) {
      Iterator<SegmentEntry> it = this.segmentMetaData.iterator();
      while (it.hasNext()) {
        ParallelMerge t =
            new ParallelMerge("Thread: " + i, this.workPath, synchronizedMap, it.next(), it.next());
        t.start();
        threads.add(t);
        i++;
      }
    }
    for (ParallelMerge thread : threads) {
      try {
        thread.join();
      } catch (InterruptedException e) {

      }
    }
    System.out.println("Join merge");
    synchronized (this.segmentMetaData) {
      this.segmentMetaData.clear();
      this.segmentMetaData.addAll(synchronizedMap);
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

    throw new UnsupportedOperationException();
  }

  /**
   * Performs an AND boolean search on the inverted index.
   *
   * @param keywords a list of keywords in the AND query
   * @return a iterator of documents matching the query
   */
  public Iterator<Document> searchAndQuery(List<String> keywords) {
    Preconditions.checkNotNull(keywords);

    throw new UnsupportedOperationException();
  }

  /**
   * Performs an OR boolean search on the inverted index.
   *
   * @param keywords a list of keywords in the OR query
   * @return a iterator of documents matching the query
   */
  public Iterator<Document> searchOrQuery(List<String> keywords) {
    Preconditions.checkNotNull(keywords);

    throw new UnsupportedOperationException();
  }

  /** Iterates through all the documents in all disk segments. */
  public Iterator<Document> documentIterator() {
    throw new UnsupportedOperationException();
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
    throw new UnsupportedOperationException();
  }
}
