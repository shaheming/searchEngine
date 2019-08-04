package edu.uci.ics.cs221.index.inverted;

import com.google.common.base.Preconditions;
import com.google.common.collect.HashBasedTable;
import edu.uci.ics.cs221.analysis.*;
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
import java.util.stream.Collectors;

class SegmentEntry {
  private String name;
  private Integer headerLen;
  private Integer docNum;
  private Set<Integer> removedDocsIdx = new TreeSet<>();
  private Integer headerNum;

  SegmentEntry(String name, Integer headerLen, Integer headerNum, Integer docNum) {
    this.name = name;
    this.headerLen = headerLen;
    this.docNum = docNum;
    this.headerNum = headerNum;
  }

  public Integer getHeaderNum() {
    return headerNum;
  }

  Integer getDocNum() {
    return docNum;
  }

  void addRemovedDocsIdx(ArrayList<Integer> removedDocsIdx) {
    this.removedDocsIdx.addAll(removedDocsIdx);
  }

  Integer getHeaderLen() {
    return headerLen;
  }

  String serilizeRmovedList() {
    if (removedDocsIdx.size() == 0) return "" + -1 + "\n";
    else {
      String str = "";
      Iterator<Integer> it = this.removedDocsIdx.iterator();
      while (it.hasNext()) {
        str = str + it.next() + " ";
      }
      return str + "\n";
    }
  }

  InvertedIndex openInvertedList(String workPath, Compressor compressor) {
    return InvertedIndex.openInvertList(workPath, this.getName(), this.headerLen, this.headerNum)
        .setRemovedDocIdx(this.getRemovedDocsIdx())
        .setCompressor(compressor)
        .setDocNum(this.docNum);
  }

  public String getName() {
    return name;
  }

  ArrayList<Integer> getRemovedDocsIdx() {
    return new ArrayList<Integer>(removedDocsIdx);
  }
}

/**
 * This class manages an disk-based inverted index and all the documents in the inverted index.
 *
 * <p>Please refer to the project 2 wiki page for implementation guidelines.
 */
public class InvertedIndexManager {
  public static boolean flag = false;
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

  private final Map<Integer, SegmentEntry> segmentMetaData =
      Collections.synchronizedMap(new TreeMap<>());
  /**
   * Key => segmentName, val => segmentHeaderSize segmentMetaData.txt: segmentsNo., fileName,
   * headerlen, headerentrynum, documentnum
   */
  private Analyzer analyzer;

  private Compressor compressor = new DeltaVarLenCompressor();
  private InvertedIndex currInvertIndex;
  private String workPath;

  private InvertedIndexManager(String indexFolder, Analyzer analyzer) {
    this.analyzer = analyzer;
    this.currInvertIndex = new InvertedIndex(indexFolder, this.compressor);
    this.workPath = indexFolder;
  }

  private InvertedIndexManager(String indexFolder, Analyzer analyzer, Compressor compressor) {
    this.compressor = compressor;
    this.analyzer = analyzer;
    this.currInvertIndex = new InvertedIndex(indexFolder, compressor);
    this.workPath = indexFolder;
  }

  /**
   * Open an InvertedIndexManager which is already exit on the disk
   *
   * @param indexFolder
   * @param analyzer
   * @return
   */
  public static InvertedIndexManager open(
      String indexFolder, Analyzer analyzer, Compressor compressor) {
    InvertedIndexManager inv = new InvertedIndexManager(indexFolder, analyzer, compressor);
    loadMetaData(inv, Paths.get(indexFolder + "/metadata.txt"));
    return inv;
  }

  /**
   * This function is used to load metadata about the segments, stored on the disk. Which contain
   * the basic info about each segment
   *
   * @param inv
   * @param filePath
   */
  public static void loadMetaData(InvertedIndexManager inv, Path filePath) {
    try {
      List<String> lines = Files.readAllLines(filePath);
      int TOTAL = Integer.valueOf(lines.get(0).split("\\s")[0]);
      int index = 0;
      for (String line : lines.subList(1, lines.size())) {
        String[] cols = line.split("\\s");
        if (inv.segmentMetaData.size() < TOTAL) {

          inv.segmentMetaData.put(
              Integer.valueOf(cols[0]),
              new SegmentEntry(
                  cols[1],
                  Integer.valueOf(cols[2]),
                  Integer.valueOf(cols[3]),
                  Integer.valueOf(cols[4])));
          System.out.println("read segment: " + cols[0] + " " + cols[1] + " info");
        } else {
          ArrayList<Integer> removedDocs = new ArrayList<>();
          for (String col : cols) {
            if (Integer.valueOf(col) == -1) break;
            else removedDocs.add(Integer.valueOf(col));
          }
          inv.segmentMetaData.get(index).addRemovedDocsIdx(removedDocs);
          index++;
        }
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /** Creates an inverted index manager with the folder and an analyzer */
  public static InvertedIndexManager createOrOpen(String indexFolder, Analyzer analyzer) {
    flag = false;
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
   * Creates a positional index with the given folder, analyzer, and the compressor. Compressor must
   * be used to compress the inverted lists and the position lists.
   */
  public static InvertedIndexManager createOrOpenPositional(
      String indexFolder, Analyzer analyzer, Compressor compressor) {
    flag = true;
    try {
      Path indexFolderPath = Paths.get(indexFolder);
      if (Files.exists(indexFolderPath) && Files.isDirectory(indexFolderPath)) {
        if (Files.isDirectory(indexFolderPath)) {
          return new InvertedIndexManager(indexFolder, analyzer, compressor);
        } else {
          throw new RuntimeException(indexFolderPath + " already exists and is not a directory");
        }
      } else {
        Files.createDirectories(indexFolderPath);
        return new InvertedIndexManager(indexFolder, analyzer, compressor);
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * Performs a phrase search on a positional index. Phrase search means the document must contain
   * the consecutive sequence of keywords in exact order.
   *
   * <p>You could assume the analyzer won't convert each keyword into multiple tokens. Throws
   * UnsupportedOperationException if the inverted index is not a positional index.
   *
   * @param phrase, a consecutive sequence of keywords
   * @return a iterator of documents matching the query
   */
  public Iterator<Document> searchPhraseQuery(List<String> phrase) {
    if (!flag) throw new UnsupportedOperationException();
    Preconditions.checkNotNull(phrase);
    StringJoiner joiner = new StringJoiner(" ");
    for (String p : phrase) {
      joiner.add(p);
    }
    List<String> newkey = this.analyzer.analyze(joiner.toString());
    Map<String, Document> result = new TreeMap<>();
    int order = 0;
    for (SegmentEntry entry : this.segmentMetaData.values()) {
      Map<String, Document> dos =
          entry
              .openInvertedList(this.workPath, this.compressor)
              .setSearchOrder(order)
              .searchPhrase(newkey);
      order++;
      result.putAll(dos);
    }
    List<Document> res = new ArrayList<>();

    for (Map.Entry<String, Document> entry : result.entrySet()) {
      res.add(entry.getValue());
    }

    return res.iterator();
  }

  /**
   * Reads a disk segment of a positional index into memory based on segmentNum. This function is
   * mainly used for checking correctness in test cases.
   *
   * <p>Throws UnsupportedOperationException if the inverted index is not a positional index.
   *
   * @param segmentNum n-th segment in the inverted index (start from 0).
   * @return in-memory data structure with all contents in the index segment, null if segmentNum
   *     don't exist.
   */
  public PositionalIndexSegmentForTest getIndexSegmentPositional(int segmentNum) {
    try {
      if (this.segmentMetaData.isEmpty()) {
        loadMetaData(this, Paths.get(this.workPath + "/metadata.txt"));
      }
    } catch (UncheckedIOException e) {
      System.out.println(e);
      return null;
    }

    if (!this.segmentMetaData.containsKey(segmentNum)) return null;
    SegmentEntry entry = this.segmentMetaData.get(segmentNum);
    try {
      InvertedIndex inv = entry.openInvertedList(this.workPath, this.compressor);
      PositionalIndexSegmentForTest res =
          new PositionalIndexSegmentForTest(
              inv.getAllInvertList(), inv.getAllDocuments(), inv.getAllpositionList());
      inv.close();
      return res;
    } catch (RuntimeException e) {
      e.printStackTrace();
      return new PositionalIndexSegmentForTest(
          new HashMap<>(), new HashMap<>(), HashBasedTable.create());
    }
  }

  /**
   * Adds a document to the inverted index. Document should live in a in-memory buffer until
   * `flush()` is called to write the segment to disk.
   *
   * @param document
   */
  public void addDocument(Document document) {
    Map<String, ArrayList<Integer>> wordPositionMap = new TreeMap<>();
    List<String> tokens = this.analyzer.analyze(document.getText());
    int index = 0;
    Iterator<String> it = tokens.iterator();
    String token = "";
    while (it.hasNext()) {
      token = it.next();
      if (!wordPositionMap.containsKey(token)) {
        ArrayList<Integer> positionIndex = new ArrayList<>();
        positionIndex.add(index);
        wordPositionMap.put(token, positionIndex);
      } else {
        wordPositionMap.get(token).add(index);
      }
      index++;
    }
    this.currInvertIndex.addDocument(document, wordPositionMap);
    if (this.currInvertIndex.getDocNum() >= DEFAULT_FLUSH_THRESHOLD) this.flush();
  }

  /**
   * Flushes all the documents in the in-memory segment buffer to disk. If the buffer is empty, it
   * should not do anything. flush() writes the segment to disk containing the posting list and the
   * corresponding document store. calculate the metadate of the of the invertList create a new
   * invertList can continue to add document
   */
  public void flush() {
    if (this.currInvertIndex.getDocNum() == 0) return;
    InvertedIndex oldInvertList = this.currInvertIndex;
    oldInvertList.flush();

    this.segmentMetaData.put(
        this.segmentMetaData.size(),
        new SegmentEntry(
            oldInvertList.getSegmentName(),
            oldInvertList.getHeaderLen(),
            oldInvertList.getHeaderNum(),
            oldInvertList.getDocNum()));
    this.writeIndexMetaData();
    this.currInvertIndex = new InvertedIndex(this.workPath, this.compressor);

    if (this.segmentMetaData.size() >= DEFAULT_MERGE_THRESHOLD) {
      //      Runnable task =
      //          () -> {
      //            this.mergeAllSegments();
      //          };
      //      task.run();
      this.mergeAllSegments();
    }
  }

  /** write metadata to disk */
  private void writeIndexMetaData() {
    try {
      BufferedWriter writer = new BufferedWriter(new FileWriter(this.workPath + "/metadata.txt"));
      String removedList = "";
      synchronized (this.segmentMetaData) {
        writer.write(this.segmentMetaData.size() + "\n");
        for (Map.Entry<Integer, SegmentEntry> entry : this.segmentMetaData.entrySet()) {
          writer.write(
              entry.getKey()
                  + " "
                  + entry.getValue().getName()
                  + " "
                  + entry.getValue().getHeaderLen()
                  + " "
                  + entry.getValue().getHeaderNum()
                  + " "
                  + entry.getValue().getDocNum()
                  + "\n");
          removedList = removedList + entry.getValue().serilizeRmovedList();
        }
      }
      writer.write(removedList);
      writer.close();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public void mergeAllSegments() {
    // merge only happens at even number of segments
    Preconditions.checkArgument(getNumSegments() % 2 == 0);
    if (this.segmentMetaData.isEmpty()) {
      loadMetaData(this, Paths.get(this.workPath + "/metadata.txt"));
    }
    Map<Integer, SegmentEntry> synchronizedMap = Collections.synchronizedMap(new TreeMap<>());
    // do some thing
    if (this.segmentMetaData.size() > 2) {
      int desSegId = 0;
      ExecutorService exec = Executors.newFixedThreadPool(4);
      synchronized (this.segmentMetaData) {
        Iterator<Map.Entry<Integer, SegmentEntry>> it = this.segmentMetaData.entrySet().iterator();
        while (it.hasNext()) {
          exec.submit(
              new ParallelMerge(
                  "Thread: " + desSegId,
                  this.workPath,
                  synchronizedMap,
                  it.next().getValue(),
                  it.next().getValue(),
                  desSegId,
                  this.compressor));
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
    } else {
      Iterator<Map.Entry<Integer, SegmentEntry>> it = this.segmentMetaData.entrySet().iterator();
      SegmentEntry s1 = it.next().getValue();
      SegmentEntry s2 = it.next().getValue();
      InvertedIndex inv1 = s1.openInvertedList(this.workPath, this.compressor);
      InvertedIndex inv2 = s2.openInvertedList(this.workPath, this.compressor);
      InvertedIndex inv = inv1.merge(inv2);
      SegmentEntry entry =
          new SegmentEntry(
              inv.getSegmentName(), inv.getHeaderLen(), inv.getHeaderNum(), inv.getDocNum());

      synchronized (synchronizedMap) {
        synchronizedMap.put(0, entry);
      }
    }

    ArrayList<String> oldFiles = new ArrayList<>();
    System.out.println("Join merge");
    synchronized (this.segmentMetaData) {
      segmentMetaData.forEach((key, val) -> oldFiles.add(val.getName()));
      this.segmentMetaData.clear();
      this.segmentMetaData.putAll(synchronizedMap);
      this.writeIndexMetaData();
    }
    oldFiles.forEach(
        (name) -> {
          try {
            Files.deleteIfExists(Paths.get(this.workPath + "/" + name + ".list"));
            Files.deleteIfExists(Paths.get(this.workPath + "/" + name + ".plist"));
            Files.deleteIfExists(Paths.get(this.workPath + "/" + name + ".db"));
          } catch (IOException e) {
            System.out.println(e.toString());
          }
        });
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
   * parallel search segments
   *
   * @param keywords
   * @param searchMethod
   * @return
   */
  private Iterator<Document> parallelSearchQuery(List<String> keywords, String searchMethod) {
    Preconditions.checkNotNull(keywords);
    Set<String> wordSet = new HashSet<>(keywords); // remove duplicated

    String words = String.join(" ", new LinkedList<String>(wordSet));
    ArrayList<String> token = new ArrayList<>(this.analyzer.analyze(words));
    ExecutorService exec = Executors.newFixedThreadPool(2);
    Map<String, Document> synchronizedMap = Collections.synchronizedMap(new TreeMap<>());
    synchronized (this.segmentMetaData) {
      for (SegmentEntry entry : this.segmentMetaData.values()) {
        Runnable runnableTask =
            () -> {
              Map<String, Document> dos =
                  entry
                      .openInvertedList(this.workPath, this.compressor)
                      .searchQuery(token, searchMethod);
              synchronized (synchronizedMap) {
                synchronizedMap.putAll(dos);
              }
            };
        exec.execute(runnableTask);
      }
    }
    exec.shutdown();
    try {

      while (!exec.awaitTermination(1L, TimeUnit.HOURS)) {
        System.out.println("Not yet. Still waiting for termination");
      }

    } catch (Exception e) {
    }
    ArrayList<Document> res = new ArrayList<>(synchronizedMap.values());
    return res.iterator();
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
    if (this.segmentMetaData.isEmpty()) {
      loadMetaData(this, Paths.get(this.workPath + "/metadata.txt"));
    }
    ArrayList<Document> docs = new ArrayList<>();
    for (SegmentEntry entry : this.segmentMetaData.values()) {
      InvertedIndex inv = entry.openInvertedList(this.workPath, this.compressor);
      docs.addAll(inv.getAllDocuments().values());
      inv.close();
    }
    this.segmentMetaData.clear();
    return docs.iterator();
  }

  /**
   * Performs top-K ranked search using TF-IDF. Returns an iterator that returns the top K documents
   * with highest TF-IDF scores.
   *
   * <p>Each element is a pair of <Document, Double (TF-IDF Score)>.
   *
   * <p>If parameter `topK` is null, then returns all the matching documents.
   *
   * <p>Unlike Boolean Query and Phrase Query where order of the documents doesn't matter, for
   * ranked search, order of the document returned by the iterator matters.
   *
   * @param keywords, a list of keywords in the query
   * @param topK, number of top documents weighted by TF-IDF, all documents if topK is null
   * @return a iterator of top-k ordered documents matching the query
   */
  public Iterator<Pair<Document, Double>> searchTfIdf(List<String> keywords, Integer topK) {
    ComposableAnalyzer ana = new ComposableAnalyzer(new PunctuationTokenizer(),new PorterStemmer());
    List<String> new_keywords;
    String str="";
    for(int i=0;i<keywords.size();i++){
      str=str+keywords.get(i)+" ";
    }
    new_keywords=ana.analyze(str);

    Integer totalDocNum = 0;
    Map<String, Integer> globalWordsDf =
        new_keywords.stream().collect(Collectors.toMap(n -> n, n -> 0, (a, b) -> b));
    ArrayList<InvertedIndex> invs = new ArrayList<>();

    // get the keywords Document frequency  in query in all segments
    for (Map.Entry<Integer, SegmentEntry> entry : this.segmentMetaData.entrySet()) {
      totalDocNum += entry.getValue().getDocNum();
      InvertedIndex inv = entry.getValue().openInvertedList(this.workPath, this.compressor);
      invs.add(inv);
      inv.accumulateKeywordsTf(globalWordsDf);
    }

    // get the global IDF for query
    final Integer total = totalDocNum;
    if(topK==null) topK=totalDocNum;
    if(topK==0) {
      List<Pair<Document, Double>> temp = new LinkedList<>();
      return temp.iterator();
    }
    Map<String, Double> globalWordsIDF =
        globalWordsDf.entrySet().stream()
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey, e -> Math.log10((double) total / e.getValue()+1)));

    Map<String, Integer> queryWordsFrequency =
        new_keywords.stream().collect(Collectors.toConcurrentMap(w -> w, w -> 1, Integer::sum));
    Map<String, Double> queryWordsTFIDF = new HashMap<>();
    // calculate the tf-idf of query
    for (Map.Entry<String, Double> entry : globalWordsIDF.entrySet()) {
      queryWordsTFIDF.put(
          entry.getKey(), entry.getValue() * queryWordsFrequency.get(entry.getKey()));
    }

    PriorityQueue<Pair<Double, Pair<Integer, InvertedIndex>>> pq;
    pq = new PriorityQueue<>(topK * 2, Comparator.comparing(Pair::getLeft));
    for (InvertedIndex inv : invs) {
      ArrayList<Pair<Double, Integer>> tfidfs =
          inv.searchTfIdf(queryWordsTFIDF, globalWordsIDF, topK);
      for (Pair<Double, Integer> tfidf : tfidfs) {
        pq.add(new Pair<>(tfidf.getLeft(), new Pair<>(tfidf.getRight(), inv)));
      }
      while (pq.size() > topK) pq.poll();
    }

    List<Pair<Document, Double>> result = new LinkedList<>();
    while (!pq.isEmpty()) {
      Pair<Double, Pair<Integer, InvertedIndex>> item = pq.poll();
      InvertedIndex inv = item.getRight().getRight();
      int docId = item.getRight().getLeft();
      result.add(new Pair<>(inv.readDocument(docId), item.getLeft()));
    }
    Collections.reverse(result);

    invs.forEach(InvertedIndex::close);

    return result.iterator();
  }

  /** Returns the total number of documents within the given segment. */
  public int getNumDocuments(int segmentNum) {
    if (this.segmentMetaData.containsKey(segmentNum)) {
      return this.segmentMetaData.get(segmentNum).getDocNum();
    } else {
      throw new RuntimeException("Wrong segment Num");
    }
  }

  /**
   * Returns the number of documents containing the token within the given segment. The token should
   * be already analyzed by the analyzer. The analyzer shouldn't be applied again.
   */
  public int getDocumentFrequency(int segmentNum, String token) {
    if (this.segmentMetaData.containsKey(segmentNum)) {
      InvertedIndex inv =
          this.segmentMetaData.get(segmentNum).openInvertedList(this.workPath, this.compressor);
      int frequency = inv.getDocumentFrequency(token);
      inv.close();
      return frequency;
    } else {
      throw new RuntimeException("Wrong segment Num");
    }
  }

  /**
   * Deletes all documents in all disk segments of the inverted index that match the query.
   *
   * @param keyword
   */
  public void deleteDocuments(String keyword) {
    if (keyword.length() == 0) return;
    String key = this.analyzer.analyze(keyword).get(0);
    ExecutorService exec = Executors.newFixedThreadPool(2);
    Map<Integer, ArrayList<Integer>> synchronizedMap = Collections.synchronizedMap(new TreeMap<>());
    if (this.segmentMetaData.isEmpty()) {
      loadMetaData(this, Paths.get(this.workPath + "/metadata.txt"));
    }
    synchronized (this.segmentMetaData) {
      for (Map.Entry<Integer, SegmentEntry> entry : this.segmentMetaData.entrySet()) {
        Runnable runnableTask =
            () -> {
              ArrayList<Integer> removedDocIds =
                  entry
                      .getValue()
                      .openInvertedList(this.workPath, this.compressor)
                      .deleteDocuments(key);
              synchronized (synchronizedMap) {
                synchronizedMap.put(entry.getKey(), removedDocIds);
              }
              this.flush();
            };
        exec.execute(runnableTask);
      }
    }
    exec.shutdown();
    try {
      while (!exec.awaitTermination(1L, TimeUnit.HOURS)) {
        System.out.println("Not yet. Still waiting for termination");
      }
    } catch (Exception e) {
    }
    synchronized (this.segmentMetaData) {
      for (Map.Entry<Integer, ArrayList<Integer>> entry : synchronizedMap.entrySet()) {
        this.segmentMetaData.get(entry.getKey()).addRemovedDocsIdx(entry.getValue());
      }
    }
    this.writeIndexMetaData();
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
      if (this.segmentMetaData.isEmpty()) {
        loadMetaData(this, Paths.get(this.workPath + "/metadata.txt"));
      }
    } catch (UncheckedIOException e) {
      System.out.println(e);
      return null;
    }

    if (!this.segmentMetaData.containsKey(segmentNum)) return null;
    SegmentEntry entry = this.segmentMetaData.get(segmentNum);
    try {
      InvertedIndex inv = entry.openInvertedList(this.workPath, this.compressor);
      InvertedIndexSegmentForTest res =
          new InvertedIndexSegmentForTest(inv.getAllInvertList(), inv.getAllDocuments());
      inv.close();
      return res;
    } catch (RuntimeException e) {
      return new InvertedIndexSegmentForTest(new HashMap<>(), new HashMap<>());
    }
  }

  /** multi-thread merge */
  static class ParallelMerge extends Thread {
    final Map<Integer, SegmentEntry> metaData;
    String put;
    InvertedIndex inv1;
    InvertedIndex inv2;
    Integer desSegId;
    InvertedIndex des;

    public ParallelMerge(
        String name,
        String path,
        final Map<Integer, SegmentEntry> metaData,
        SegmentEntry s1,
        SegmentEntry s2,
        Integer desSegId,
        Compressor compressor) {
      super(name);
      this.put = s1.getName() + " " + s2.getName();
      this.inv1 = s1.openInvertedList(path, compressor);
      this.inv2 = s2.openInvertedList(path, compressor);
      this.desSegId = desSegId;
      this.metaData = metaData;
    }

    public void run() {
      try {
        System.out.println(
            "Start merge: " + this.put + ",thread name is: " + Thread.currentThread().getName());
        this.des = inv1.merge(inv2);
        SegmentEntry entry =
            new SegmentEntry(
                des.getSegmentName(), des.getHeaderLen(), des.getHeaderNum(), des.getDocNum());

        synchronized (this.metaData) {
          this.metaData.put(this.desSegId, entry);
        }
      } catch (RuntimeException e) {
        e.printStackTrace();
      }
      System.out.println(
          "Merge: "
              + this.put
              + " To "
              + this.des.getSegmentName()
              + ",thread name is: "
              + Thread.currentThread().getName()
              + " finished");
    }
  }
}
