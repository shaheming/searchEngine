package edu.uci.ics.cs221.index.inverted;

import edu.uci.ics.cs221.storage.Document;
import edu.uci.ics.cs221.storage.DocumentStore;
import edu.uci.ics.cs221.storage.MapdbDocStore;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

class WritePageBuffer {
  private ByteBuffer buffer1 = ByteBuffer.allocate(PageFileChannel.PAGE_SIZE + 100);
  private ByteBuffer buffer2 = ByteBuffer.allocate(PageFileChannel.PAGE_SIZE + 100);
  private Integer limit = PageFileChannel.PAGE_SIZE;
  private ByteBuffer buffer = buffer1;
  private PageFileChannel fileChannel;

  /**
   * this buffer use two buffer to align the data to one page size. When the page buffer is filled,
   * it will be flush to disk automatically
   *
   * @param fileChannel pagechennel
   */
  WritePageBuffer(PageFileChannel fileChannel) {
    this.fileChannel = fileChannel;
  }

  public WritePageBuffer putInt(int val) {
    buffer.putInt(val);
    checkFull();
    return this;
  }

  public WritePageBuffer put(ByteBuffer src) {
    int n = src.remaining();
    for (int i = 0; i < n; i++) {
      if (buffer.position() >= this.limit) {
        checkFull();
      }
      buffer.put(src.get());
    }
    return this;
  }

  public WritePageBuffer put(byte b) {
    buffer.put(b);
    checkFull();
    return this;
  }

  private void checkFull() {
    if (buffer.position() >= this.limit) {
      ByteBuffer bufferOld = buffer;
      if (buffer == buffer1) {
        buffer = buffer2;
      } else {
        buffer = buffer1;
      }
      bufferOld.limit(bufferOld.position());
      bufferOld.position(limit);
      if (bufferOld.remaining() > 0) buffer.put(bufferOld);
      this.flush(bufferOld);
    }
  }

  private void flush(ByteBuffer buffer) {
    buffer.limit(PageFileChannel.PAGE_SIZE);
    buffer.position(0);
    this.fileChannel.appendPage(buffer.slice());
    buffer.clear();
  }

  public void flush() {

    this.buffer.limit(PageFileChannel.PAGE_SIZE);
    while (this.buffer.remaining() > 0) {
      this.buffer.put((byte) 0);
    }
    this.buffer.flip();
    this.fileChannel.appendPage(buffer.slice());
    this.buffer.clear();
  }
}

/**
 * This data structure is used to express the header entry in the invertedList. Each entry can
 * include | key | list length | list pointer |
 */
class InvertedIndexHeaderEntry {
  private String key;
  private Integer size;
  private Integer docIdListPtr;
  private Integer keyPtr;
  static Integer keyByteSize = 30;
  static Integer ptrByteSize = 4;
  static Integer docIdByteSize = 4;
  static Integer InvertedIndexHeaderEntrySize = ptrByteSize + ptrByteSize + docIdByteSize;

  public InvertedIndexHeaderEntry(String key, Integer size, Integer ptr) {
    this.key = key;
    this.size = size;
    this.docIdListPtr = ptr;
  }

  InvertedIndexHeaderEntry() {}

  public void setKeyPtr(Integer keyPtr) {
    this.keyPtr = keyPtr;
  }

  public Integer getKeyPtr() {
    return keyPtr;
  }

  public void getKeyFromHeader(String words, Integer lastKeyPtr) {
    this.key = words.substring( this.keyPtr,lastKeyPtr);
  }

  public void setKey(String key) {
    this.key = key;
  }

  public void setSize(Integer size) {
    this.size = size;
  }

  void setDocIdListPtr(Integer docIdListPtr) {
    this.docIdListPtr = docIdListPtr;
  }

  public String getKey() {
    return key;
  }

  public Integer getSize() {
    return size;
  }

  Integer getDocIdListPtr() {
    return docIdListPtr;
  }

  /**
   * This is use to construct the entry from byte array
   *
   * @param buffer is the buffer is the page buffer. for every entry this function read the length
   *     of entry size
   */
  InvertedIndexHeaderEntry(ByteBuffer buffer) {
    this.keyPtr = buffer.getInt();
    this.size = buffer.getInt();
    this.docIdListPtr = buffer.getInt();
    //    this.print();
  }

  void print() {
    System.out.println(this.key + " " + this.size + " " + this.docIdListPtr);
  }

  /** @param buffer convert the entry to byte and write it to write buffer */
  void convertToByte(WritePageBuffer buffer) {

    //    for (int i = 0; i < this.key.length(); i++) {
    //      buffer.put((byte) this.key.charAt(i));
    //    }
    //    for (int i = this.key.length(); i < InvertedIndexHeaderEntry.keyByteSize; i++) {
    //      buffer.put((byte) 0);
    //    }
    buffer.putInt(this.keyPtr);
    buffer.putInt(this.size);
    buffer.putInt(this.docIdListPtr);
  }
}

/** This class is the data structure of the invertedIndex in the memory */
public class InvertedIndex {
  private Integer docNum = 0;
  private String workPath;
  private String invertListPath;
  private String invertListDir;
  private String docStorePath;
  private String docStoreDir;
  private Integer headerLen = 0; // Byte
  private DocumentStore docStore;
  protected Map<Integer, Document> documents = new HashMap<>();
  private Map<String, List<Integer>> invertList = new TreeMap<>();
  private PageFileChannel fileChannel;
  private String segmentName;
  private Integer headerNum = 0;
  private Map<String, InvertedIndexHeaderEntry> wordsDicEntries = new TreeMap<>();
  private Set<Integer> removedDocIdx = new TreeSet<>();

  public InvertedIndex setRemovedDocIdx(ArrayList<Integer> removedList) {
    this.removedDocIdx = new TreeSet<>(removedList);
    return this;
  }

  private void InitFilePath(String dir, String name) {
    this.workPath = dir;
    this.invertListDir = this.workPath + "/";
    this.docStoreDir = this.workPath+ "/";
    this.segmentName = name;
    this.invertListPath = this.invertListDir + this.segmentName + ".list";
    this.docStorePath = this.docStoreDir + this.segmentName + ".db";
  }

  /** @param workPath the path of work dir */
  public InvertedIndex(String workPath) {
    InitFilePath(workPath, getTimeStamp());
    this.checkAndCreateDir(this.invertListDir);
    this.checkAndCreateDir(this.docStoreDir);
  }

  private void checkAndCreateDir(String path) {
    File directory = new File(path);
    if (!directory.exists()) {
      directory.mkdirs();
    }
  }

  /**
   * @param workPath path this method is used to initialize the invertedIndex on the disk and open
   *     relative file for read the
   */
  private InvertedIndex(String workPath, String fileName, Integer headerLen, Integer headerNum) {
    InitFilePath(workPath, fileName);
    this.headerLen = headerLen;
    this.headerNum = headerNum;
    Path indexPath = Paths.get(this.invertListPath);
    Path indexDocPath = Paths.get(this.docStorePath);
    if (this.fileChannel == null) {
      this.fileChannel = PageFileChannel.createOrOpen(Paths.get(this.invertListPath));
    }

    if (!Files.exists(indexPath)) {
      throw new RuntimeException(indexPath + " do not find");

    } else if (!Files.exists(indexDocPath)) {
      throw new RuntimeException(indexDocPath + " do not find");
    }
    this.openReadOnlyDocDb();
  }

  public void close() {
    if (this.fileChannel != null) {
      this.fileChannel.close();
    }
    if (this.docStore != null) {
      this.docStore.close();
    }
  }

  private void calHeaderLen() {
    this.headerLen =
        this.invertList.size() * (InvertedIndexHeaderEntry.InvertedIndexHeaderEntrySize);
  }

  /**
   * todo flush invertList to disk(create the invertList file) flush docStore to disk
   *
   * @return
   */
  public boolean flush() {
    if (this.invertList.isEmpty() && this.documents.isEmpty()) return false;

    if (this.fileChannel == null) {
      this.fileChannel = PageFileChannel.createOrOpen(Paths.get(this.invertListPath));
    }
    WritePageBuffer writeBuffer = new WritePageBuffer(this.fileChannel);

    if (this.docStore == null)
      this.docStore =
          MapdbDocStore.createWithBulkLoad(this.docStorePath, this.documents.entrySet().iterator());
    // 1.cal head size
    // 2.write head
    // 3.write list
    StringBuilder builder = new StringBuilder();

    for (String s : this.invertList.keySet()) {
      builder.append(s);
    }
    // write words dic to file
    String wordsStr = builder.toString();
    ByteBuffer buffer = ByteBuffer.allocate(wordsStr.length());
    for (int i = 0; i < wordsStr.length(); i++) {
      writeBuffer.put((byte) wordsStr.charAt(i));
    }

    this.headerLen =
        wordsStr.length()
            + this.invertList.size() * InvertedIndexHeaderEntry.InvertedIndexHeaderEntrySize;

    Integer curListPtr = this.headerLen;
    Integer curWordPtr = 0;

    InvertedIndexHeaderEntry headerEntry = new InvertedIndexHeaderEntry();

    for (Map.Entry<String, List<Integer>> entry : this.invertList.entrySet()) {
      headerEntry.setKey(entry.getKey());
      headerEntry.setSize(entry.getValue().size());
      headerEntry.setKeyPtr(curWordPtr);
      headerEntry.setDocIdListPtr(curListPtr);
      curListPtr += entry.getValue().size() * InvertedIndexHeaderEntry.ptrByteSize;
      curWordPtr += entry.getKey().length();
      headerEntry.convertToByte(writeBuffer);
    }

    for (Map.Entry<String, List<Integer>> entry : this.invertList.entrySet()) {
      for (Integer i : entry.getValue()) {
        writeBuffer.putInt(i);
      }
    }

    writeBuffer.flush();
    System.out.println("Write segment: " + this.segmentName + " docNum: " + this.docStore.size());

    this.close();
    return true;
  }

  private boolean openReadOnlyDocDb() {
    if (this.docStore != null) return false;
    else {
      this.docStore = MapdbDocStore.createOrOpenReadOnly(this.docStorePath);
      return true;
    }
  }

  private static Integer copyDocToRemoveDelete(
      InvertedIndex src, InvertedIndex des, Deque<Integer> removedDq) {
    Iterator<Map.Entry<Integer, Document>> it = src.docStore.iterator();
    int counter = 0;
    while (it.hasNext()) {
      Map.Entry<Integer, Document> entry = it.next();
      if ((removedDq.size() > 0) && removedDq.peek() <= entry.getKey()) {
        removedDq.poll();
        continue;
      }
      des.addDocumentForMerge(entry.getValue());
      counter++;
    }
    return counter;
  }

  private static Integer copyDocIdToRemoveDelete(
      InvertedIndex des,
      String key,
      Deque<Integer> removedDq,
      ArrayList<Integer> ids,
      Integer offset) {
    int counter = 0;
    ArrayList<Integer> newDocIds = new ArrayList<>();
    for (int i = 0; i < ids.size(); i++) {
      if ((removedDq.size() > 0) && removedDq.peek() <= ids.get(i)) {
        removedDq.poll();
        counter++;
        continue;
      }
      newDocIds.add(ids.get(i) - counter + offset);
    }
    if (des.invertList.containsKey(key)) {
      des.invertList.get(key).addAll(newDocIds);
      return des.invertList.get(key).size();
    } else {
      if (!newDocIds.isEmpty()) des.invertList.put(key, newDocIds);
      return 0;
    }
  }

  /**
   * merge other inverted list to 'this' This method use the dq to iterate the keywords in two
   * inverted list during the merge process. every queue is the sorted keywords. when merge the
   * keywords to new invertedlist we should filter the deleted documents and recalculate the local
   * docId this function first merge the documents db, add get an offset which is the start number
   * of the second invertedlist's local doc id in the new invertedlist
   *
   * @param inv another invertedlist
   * @return new invertedlist
   */
  public InvertedIndex merge(InvertedIndex inv) {
    InvertedIndex des = new InvertedIndex(this.workPath);
    this.openReadOnlyDocDb();
    inv.openReadOnlyDocDb();
    // db1
    Integer ivL1Size = copyDocToRemoveDelete(this, des, new LinkedList<>(this.getRemovedDocIdx()));
    Integer ivL2Size = copyDocToRemoveDelete(inv, des, new LinkedList<>(inv.getRemovedDocIdx()));

    // invertList
    this.readHeader();
    inv.readHeader();

    Deque<Map.Entry<String, InvertedIndexHeaderEntry>> dq1 =
        new LinkedList<>(this.wordsDicEntries.entrySet());
    Deque<Map.Entry<String, InvertedIndexHeaderEntry>> dq2 =
        new LinkedList<>(inv.wordsDicEntries.entrySet());
    InvertedIndexHeaderEntry entry1;
    InvertedIndexHeaderEntry entry2;

    while (!dq1.isEmpty() && !dq2.isEmpty()) {
      entry1 = dq1.peek().getValue();
      entry2 = dq2.peek().getValue();
      int compare = entry1.getKey().compareTo(entry2.getKey());
      if (compare < 0) {
        copyDocIdToRemoveDelete(
            des,
            entry1.getKey(),
            new LinkedList<>(this.getRemovedDocIdx()),
            this.readDocIds(entry1),
            0);
        dq1.poll();

      } else if (compare > 0) {
        copyDocIdToRemoveDelete(
            des,
            entry2.getKey(),
            new LinkedList<>(inv.getRemovedDocIdx()),
            inv.readDocIds(entry2),
            ivL1Size);
        dq2.poll();
      } else {
        int offset =
            copyDocIdToRemoveDelete(
                des,
                entry1.getKey(),
                new LinkedList<>(this.getRemovedDocIdx()),
                this.readDocIds(entry1),
                0);
        copyDocIdToRemoveDelete(
            des,
            entry2.getKey(),
            new LinkedList<>(inv.getRemovedDocIdx()),
            inv.readDocIds(entry2),
            ivL1Size);
        dq1.poll();
        dq2.poll();
      }
    }
    while (!dq1.isEmpty()) {

      entry1 = dq1.peek().getValue();
      copyDocIdToRemoveDelete(
          des,
          entry1.getKey(),
          new LinkedList<>(this.getRemovedDocIdx()),
          this.readDocIds(entry1),
          0);
      dq1.poll();
    }
    while (!dq2.isEmpty()) {
      entry2 = dq2.peek().getValue();
      copyDocIdToRemoveDelete(
          des,
          entry2.getKey(),
          new LinkedList<>(inv.getRemovedDocIdx()),
          this.readDocIds(entry2),
          ivL1Size);
      dq2.poll();
    }

    des.flush();
    this.close();
    inv.close();
    des.close();
    return des;
  }

  /**
   * @param indexFolder segment folder
   * @param indexName segment name use time stamp
   * @return InvertedIndex
   */
  public static InvertedIndex openInvertList(
      String indexFolder, String indexName, Integer headerLen, Integer headerNum) {

    InvertedIndex inv = new InvertedIndex(indexFolder, indexName, headerLen, headerNum);

    return inv;
  }

  /**
   * this method will add document
   *
   * @param document
   */
  public void addDocumentForMerge(Document document) {
    if (this.docNum < InvertedIndexManager.DEFAULT_FLUSH_THRESHOLD)
      this.documents.put(this.docNum, document);
    else if (this.docStore == null) {
      this.documents.put(this.docNum, document);
      this.docStore =
          MapdbDocStore.createWithBulkLoad(this.docStorePath, this.documents.entrySet().iterator());
      this.documents.clear();
    } else {
      this.docStore.addDocument(this.docNum, document);
    }
    this.docNum++;
  }

  /**
   * add document and token to the invertedlist this will filter the invalid keywords, which exceed
   * the MAX length of keyword
   *
   * @param document
   * @param tokens
   */
  public void addDocumentForMerge(Document document, Set<String> tokens) {
    this.documents.put(this.docNum, document);
    for (String key : tokens) {
//      if (key.length() == 0 || key.length() > InvertedIndexHeaderEntry.keyByteSize) continue;
      if (this.invertList.containsKey(key)) {
        invertList.get(key).add(this.docNum);
      } else {
        invertList.put(key, new ArrayList<Integer>(Arrays.asList(this.docNum)));
      }
    }
    this.docNum++;
  }

  public Integer getDocNum() {
    return this.docNum;
  }

  public String getSegmentName() {
    return segmentName;
  }

  public Integer getHeaderLen() {
    return headerLen;
  }

  /** deserialize the header */
  public void readHeader() {
    if (this.fileChannel == null) {
      this.fileChannel = PageFileChannel.createOrOpen(Paths.get(this.invertListPath));
    }
    Integer pageNum = (this.headerLen + PageFileChannel.PAGE_SIZE - 1) / PageFileChannel.PAGE_SIZE;
    ByteBuffer headerBuffer =
        ByteBuffer.allocate(InvertedIndexHeaderEntry.InvertedIndexHeaderEntrySize);
    StringBuilder builder = new StringBuilder();
    Integer wordsLen =
        this.headerLen - this.headerNum * InvertedIndexHeaderEntry.InvertedIndexHeaderEntrySize;
    String words = "";
    InvertedIndexHeaderEntry lastEntry = null;
    Integer counter = 0;
    for (int i = 0; i < pageNum; i++) {
      ByteBuffer buffer = this.fileChannel.readPage(i);
      // read header words
      if (counter < wordsLen) {
        while (buffer.hasRemaining()) {
          builder.append((char) buffer.get());
          counter++;
          if (counter.equals(wordsLen)) break;
        }
        if (counter < wordsLen) continue;
        else {
          words = builder.toString();
        }
      }

      // read remained page
      if (headerBuffer.position() > 0) {
        while (headerBuffer.remaining() > 0) {
          headerBuffer.put(buffer.get());
        }
        headerBuffer.flip();
        InvertedIndexHeaderEntry entry = new InvertedIndexHeaderEntry(headerBuffer);
        if (lastEntry != null) {
          lastEntry.getKeyFromHeader(words, entry.getKeyPtr());
          wordsDicEntries.put(lastEntry.getKey(), lastEntry);
        }

        lastEntry = entry;
        headerBuffer.clear();
        if (wordsDicEntries.size() == this.headerNum - 1) {
          lastEntry.getKeyFromHeader(words, wordsLen);
          wordsDicEntries.put(lastEntry.getKey(), lastEntry);
          return;
        }
      }

      while (buffer.remaining() >= InvertedIndexHeaderEntry.InvertedIndexHeaderEntrySize) {
        InvertedIndexHeaderEntry entry = new InvertedIndexHeaderEntry(buffer);
        if (lastEntry != null) {
          lastEntry.getKeyFromHeader(words, entry.getKeyPtr());
          wordsDicEntries.put(lastEntry.getKey(), lastEntry);
        }
        lastEntry = entry;
        if (wordsDicEntries.size() == this.headerNum - 1) {
          lastEntry.getKeyFromHeader(words, wordsLen);
          wordsDicEntries.put(lastEntry.getKey(), lastEntry);
          return;
        }
      }
      // System.out.println("headerCounter: " + wordsDicEntries.size());
      if (buffer.remaining() > 0) {
        headerBuffer.put(buffer);
      }
    }

    this.headerNum = this.wordsDicEntries.size();
  }

  /**
   * parallel reading the document indexes from the file
   *
   * @param words
   * @return
   */
  public Map<String, ArrayList<Integer>> readWordsDocIdx(ArrayList<String> words) {
    // System.out.println("Search words: " + Arrays.toString(words.toArray()));
    Map<String, ArrayList<Integer>> synchronizedMap = Collections.synchronizedMap(new HashMap<>());

    ExecutorService exec = Executors.newFixedThreadPool(4);

    for (String word : words) {
      if (this.wordsDicEntries.containsKey(word)) {
        final InvertedIndexHeaderEntry entry = this.wordsDicEntries.get(word);
        Runnable runnableTask =
            () -> {
              ArrayList<Integer> idx = this.readDocIds(entry);
              synchronized (synchronizedMap) {
                synchronizedMap.put(entry.getKey(), idx);
              }
            };
        exec.execute(runnableTask);
      } else {
        synchronized (synchronizedMap) {
          synchronizedMap.put(word, new ArrayList<>());
        }
      }
    }
    exec.shutdown();
    try {
      while (!exec.awaitTermination(1L, TimeUnit.MINUTES)) {
        System.out.println("Not yet. Still waiting for termination");
      }

    } catch (InterruptedException e) {

    }
    return synchronizedMap;
  }

  /**
   * do or operation to all inverted list
   *
   * @param map
   * @return
   */
  private BitSet andAllIndex(Map<String, ArrayList<Integer>> map) {
    int size = getDocNumFromDb();
    BitSet checker = new BitSet(size);
    checker.set(0, size, true);

    for (Map.Entry<String, ArrayList<Integer>> entry : map.entrySet()) {
      BitSet tmpChecker = new BitSet(size);
      for (int idx : entry.getValue()) {
        tmpChecker.set(idx);
      }
      checker.and(tmpChecker);
    }
    for (int i : this.removedDocIdx) {
      checker.set(i, false);
    }
    return checker;
  }

  private BitSet orAllIndex(Map<String, ArrayList<Integer>> map) {
    int size = getDocNumFromDb();
    BitSet checker = new BitSet(size);
    for (Map.Entry<String, ArrayList<Integer>> entry : map.entrySet()) {
      BitSet tmpChecker = new BitSet(size);
      for (int idx : entry.getValue()) {
        tmpChecker.set(idx);
      }
      checker.or(tmpChecker);
    }
    for (int i : this.removedDocIdx) {
      checker.set(i, false);
    }
    return checker;
  }

  public Map<String, Document> searchQuery(ArrayList<String> words, String query) {
    if (words.size() == 0) return new HashMap<>();

    // read header
    this.readHeader();
    //    System.out.println("Search " + Arrays.toString(words.toArray()) + " " + query);
    Map<String, ArrayList<Integer>> map = new HashMap<>();
    // search header
    try {
      map.putAll(this.readWordsDocIdx(words));
    } catch (Exception e) {
      return new HashMap<>();
    }

    if (map.size() == 0) return new HashMap<>();

    BitSet checker;
    if (query.equals("AND")) checker = andAllIndex(map);
    else if (query.equals("OR")) {
      checker = orAllIndex(map);
    } else {
      throw new RuntimeException("No match query method");
    }
    ArrayList<Integer> docIdx = new ArrayList<>();
    for (int i = 0; i < checker.size(); i++) {
      if (checker.get(i)) {
        docIdx.add(i);
      }
    }
    System.out.println(
        "Search "
            + Arrays.toString(words.toArray())
            + " "
            + query
            + " "
            + this.segmentName
            + ", find "
            + docIdx.size()
            + " docs");

    try {
      Map<String, Document> res = this.readDocuments(docIdx);
      return res;
    } catch (Exception e) {
      System.out.println("read docs idx: " + Arrays.toString(docIdx.toArray()) + " failed");
      return new HashMap<>();
    } finally {
      this.close();
    }
  }

  public Map<String, Document> readDocuments(ArrayList<Integer> idex) throws InterruptedException {
    this.openReadOnlyDocDb();
    Map<String, Document> synchronizedMap = Collections.synchronizedMap(new HashMap<>());

    ExecutorService exec = Executors.newFixedThreadPool(4);
    for (int id : idex) {
      Runnable runnableTask =
          () -> {
            Document doc = this.docStore.getDocument(id);
            synchronized (synchronizedMap) {
              synchronizedMap.put("id_" + id + "_" + this.segmentName, doc);
            }
          };
      exec.execute(runnableTask);
    }
    exec.shutdown();
    while (!exec.awaitTermination(1L, TimeUnit.HOURS)) {
      System.out.println("Not yet. Still waiting for termination");
    }

    return synchronizedMap;
  }

  private ArrayList<Integer> readDocIds(InvertedIndexHeaderEntry entry) {
    int pageStart = entry.getDocIdListPtr() / PageFileChannel.PAGE_SIZE;
    int pageEnd =
        (entry.getDocIdListPtr()
                + entry.getSize() * InvertedIndexHeaderEntry.docIdByteSize
                + PageFileChannel.PAGE_SIZE
                - 1)
            / PageFileChannel.PAGE_SIZE;
    int offset =
        pageStart > 0
            ? entry.getDocIdListPtr() - pageStart * PageFileChannel.PAGE_SIZE
            : entry.getDocIdListPtr();

    ArrayList<Integer> docIds = new ArrayList<>();
    ByteBuffer intBuffer = ByteBuffer.allocate(InvertedIndexHeaderEntry.docIdByteSize);
    for (int i = pageStart; i <= pageEnd; i++) {
      ByteBuffer buffer = this.fileChannel.readPage(i);
      if (i == pageStart) {
        buffer.position(offset);
      }
      if (intBuffer.position() > 0) {
        while (intBuffer.remaining() > 0) {
          intBuffer.put(buffer.get());
        }
        intBuffer.flip();
        docIds.add(intBuffer.getInt());
        intBuffer.clear();
        if (docIds.size() == entry.getSize()) {
          return docIds;
        }
      }
      while (buffer.remaining() >= InvertedIndexHeaderEntry.docIdByteSize) {
        docIds.add(buffer.getInt());
        if (docIds.size() == entry.getSize()) {
          return docIds;
        }
      }
      while (buffer.hasRemaining()) {
        intBuffer.put(buffer.get());
      }
    }
    return docIds;
  }

  public LinkedList<Integer> getRemovedDocIdx() {
    return new LinkedList<>(removedDocIdx);
  }

  public int getDocNumFromDb() {
    if (this.docStore == null) throw new RuntimeException("Db is not been open");
    return (int) this.docStore.size();
  }
  // random used to avoid multi thread conflicts when creating the segment file
  private String getTimeStamp() {
    return (new Timestamp(System.currentTimeMillis()))
            .toString()
            .replace(" ", "_")
            .replace("-", "")
            .replace(":", "")
            .replace(".", "")
        + "_"
        + (int) (Math.random() * 1000 + 1) % 1000;
  }

  public Map<String, List<Integer>> getAllInvertList() {
    if (this.wordsDicEntries.size() == 0) {
      this.readHeader();
    }
    for (InvertedIndexHeaderEntry entry : this.wordsDicEntries.values()) {
      this.invertList.put(entry.getKey(), this.readDocIds(entry));
    }
    this.fileChannel.close();
    return this.invertList;
  }

  public Map<Integer, Document> getAllDocuments() {
    if (this.docStore == null) this.openReadOnlyDocDb();
    Map<Integer, Document> docs = new HashMap<>();
    for (int i = 0; i < (int) this.docStore.size(); i++) {
      if (!this.removedDocIdx.contains(i)) docs.put(i, this.docStore.getDocument(i));
    }
    this.docStore.close();
    return docs;
  }

  public ArrayList<Integer> deleteDocuments(String keyword) {
    ArrayList<String> keywords = new ArrayList<>();

    keywords.add(keyword);

    if (this.wordsDicEntries.isEmpty()) {
      this.readHeader();
    }
    try {
      Map<String, ArrayList<Integer>> wordsDocMap = this.readWordsDocIdx(keywords);
      for (Map.Entry<String, ArrayList<Integer>> entry : wordsDocMap.entrySet()) {
        this.removedDocIdx.addAll(entry.getValue());
      }

    } catch (Exception e) {
      System.err.println("Delete keyword " + keyword + " errors!");
      return new ArrayList<>();
    }
    return new ArrayList<>(this.removedDocIdx);
  }

  public Integer getHeaderNum() {
    return this.headerNum = this.invertList.size();
  }
}
