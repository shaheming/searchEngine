package edu.uci.ics.cs221.index.inverted;

import com.google.common.base.Preconditions;
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

  WritePageBuffer(PageFileChannel fileChannel) {
    this.fileChannel = fileChannel;
  }

  public WritePageBuffer putChar(char val) {
    checkFull();
    buffer.putChar(val);
    return this;
  }

  public WritePageBuffer putInt(int val) {
    checkFull();
    buffer.putInt(val);
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
      //            System.out.println(Arrays.toString(buffer1.array()));
      //            System.out.println(Arrays.toString(buffer2.array()));

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

class InvertedIndexHeaderEntry {
  private String key;
  private Integer size;
  private Integer ptr;
  static Integer keyByteSize = 30;
  static Integer ptrByteSize = 4;
  static Integer listByteSize = 4;
  static Integer InvertedIndexHeaderEntrySize = keyByteSize + ptrByteSize + listByteSize;

  public InvertedIndexHeaderEntry(String key, Integer size, Integer ptr) {
    this.key = key;
    this.size = size;
    this.ptr = ptr;
  }

  InvertedIndexHeaderEntry() {}

  public void setKey(String key) {
    this.key = key;
  }

  public void setSize(Integer size) {
    this.size = size;
  }

  void setPtr(Integer ptr) {
    this.ptr = ptr;
  }

  public String getKey() {
    return key;
  }

  public Integer getSize() {
    return size;
  }

  Integer getPtr() {
    return ptr;
  }

  InvertedIndexHeaderEntry(ByteBuffer buffer) {
    char[] strArray = new char[InvertedIndexHeaderEntry.keyByteSize];
    for (int i = 0; i < InvertedIndexHeaderEntry.keyByteSize; i++) {
      char c = (char) buffer.get();
      if (c == '\0') {
        this.key = new String(Arrays.copyOfRange(strArray, 0, i));
        break;
      } else {
        strArray[i] = c;
        if (i == InvertedIndexHeaderEntry.keyByteSize - 1) {
          this.key = new String(Arrays.copyOfRange(strArray, 0, i + 1));
        }
      }
    }

    if (this.key == null) {
      this.key = new String(Arrays.copyOfRange(strArray, 0, InvertedIndexHeaderEntry.keyByteSize));
    }

    for (int i = this.key.length() + 1; i < InvertedIndexHeaderEntry.keyByteSize; i++) {
      buffer.get();
    }
    this.size = buffer.getInt();
    this.ptr = buffer.getInt();
    //    this.print();
  }

  void print() {
    System.out.println(this.key + " " + this.size + " " + this.ptr);
  }

  void writeToMyByteBuffer(WritePageBuffer buffer) {

    for (int i = 0; i < this.key.length(); i++) {
      buffer.put((byte) this.key.charAt(i));
    }
    for (int i = this.key.length(); i < InvertedIndexHeaderEntry.keyByteSize; i++) {
      buffer.put((byte) 0);
    }

    buffer.putInt(this.size);
    buffer.putInt(this.ptr);
  }
}

public class InvertedIndex {
  private Integer docNum = 0;
  private Map<String, List<Integer>> invertList = new TreeMap<>();
  private String invertListPath;
  private String invertListDir;
  private String docStorePath;
  private String docStoreDir;
  private Integer headerLen = 0; // Byte
  private DocumentStore docStore;
  protected Map<Integer, Document> documents = new HashMap<>();
  private PageFileChannel fileChannel;
  private String basePath;
  private String segmentName;
  private Integer headerNum = 0;
  private WritePageBuffer writeBuffer;
  private Map<String, InvertedIndexHeaderEntry> wordsDicEntries = new TreeMap<>();
  private Set<Integer> removedDocIdx = new TreeSet<>();

  public InvertedIndex setRemovedDocIdx(ArrayList<Integer> removedList) {
    this.removedDocIdx = new TreeSet<>(removedList);
    return this;
  }

  private void InitFilePath(String dir, String name) {
    this.basePath = dir;
    this.invertListDir = this.basePath + "/index/";
    this.docStoreDir = this.basePath + "/doc/";
    this.segmentName = name;
    this.invertListPath = this.invertListDir + this.segmentName + ".list";
    this.docStorePath = this.docStoreDir + this.segmentName + ".db";
  }

  /** @param dir */
  public InvertedIndex(String dir) {
    InitFilePath(dir, getTimeStamp());
    this.checkAndCreateDir(this.invertListDir);
    this.checkAndCreateDir(this.docStoreDir);
  }

  private void checkAndCreateDir(String path) {
    File directory = new File(path);
    if (!directory.exists()) {
      directory.mkdirs();
    }
  }

  /** @param dir */
  public InvertedIndex(String dir, String fileName, Integer headerLen) {
    InitFilePath(dir, fileName);
    this.headerLen = headerLen;
    this.headerNum = headerLen / InvertedIndexHeaderEntry.InvertedIndexHeaderEntrySize;
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
    this.writeBuffer = new WritePageBuffer(this.fileChannel);
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
    if (this.fileChannel == null) {
      this.fileChannel = PageFileChannel.createOrOpen(Paths.get(this.invertListPath));
    }
    this.writeBuffer = new WritePageBuffer(this.fileChannel);

    if (this.docStore == null)
      this.docStore =
          MapdbDocStore.createWithBulkLoad(this.docStorePath, this.documents.entrySet().iterator());
    // 1.cal head size
    // 2.write head
    // 3.write list
    this.calHeaderLen();
    Integer curListPtr = this.headerLen;
    InvertedIndexHeaderEntry headerEntry = new InvertedIndexHeaderEntry();
    int index = 0;
    for (Map.Entry<String, List<Integer>> entry : this.invertList.entrySet()) {
      headerEntry.setKey(entry.getKey());
      headerEntry.setSize(entry.getValue().size());
      headerEntry.setPtr(curListPtr);
      curListPtr += entry.getValue().size() * InvertedIndexHeaderEntry.ptrByteSize;
      headerEntry.writeToMyByteBuffer(this.writeBuffer);
      index++;
    }
    for (Map.Entry<String, List<Integer>> entry : this.invertList.entrySet()) {
      for (Integer i : entry.getValue()) {
        writeBuffer.putInt(i);
      }
    }
    writeBuffer.flush();
    System.out.println("Write segment: " + this.segmentName + " docNum: " + this.docStore.size());
    this.docStore.close();
    this.fileChannel.close();
    return true;
  }

  public boolean openReadOnlyDocDb() {
    if (this.docStore != null) return false;
    else {
      this.docStore = MapdbDocStore.createOrOpenReadOnly(this.docStorePath);
      return true;
    }
  }

  public Iterator<Map.Entry<Integer, Document>> docIterator() {
    return this.docStore.iterator();
  }

  private static Integer copyDocToRemoveDelete(
      InvertedIndex src, InvertedIndex des, Deque<Integer> removedDq) {
    Iterator<Map.Entry<Integer, Document>> it = src.docIterator();
    int counter = 0;
    while (it.hasNext()) {
      Map.Entry<Integer, Document> entry = it.next();
      if ((removedDq.size() > 0) && removedDq.peek() <= entry.getKey()) {
        removedDq.poll();
        continue;
      }
      des.addDocument(entry.getValue());
      counter++;
    }
    return counter;
  }

  public static Integer copyDocIdToRemoveDelete(
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

  // todo cal doc size
  // todo merge second to the first
  public InvertedIndex merge(InvertedIndex inv) {
    InvertedIndex des = new InvertedIndex(this.basePath);
    this.openReadOnlyDocDb();
    inv.openReadOnlyDocDb();
    // db1
    Integer ivL1Size = copyDocToRemoveDelete(this, des, new LinkedList<>(this.getRemovedDocIdx()));
    Integer ivL2Size = copyDocToRemoveDelete(inv, des, new LinkedList<>(inv.getRemovedDocIdx()));

    // invertList
    this.readHeader();
    inv.readHeader();

    Iterator<Map.Entry<String, InvertedIndexHeaderEntry>> it1 =
        this.wordsDicEntries.entrySet().iterator();
    Iterator<Map.Entry<String, InvertedIndexHeaderEntry>> it2 =
        inv.wordsDicEntries.entrySet().iterator();
    InvertedIndexHeaderEntry entry1;
    InvertedIndexHeaderEntry entry2;

    while (it1.hasNext() && it2.hasNext()) {
      entry1 = it1.next().getValue();
      entry2 = it2.next().getValue();
      int compare = entry1.getKey().compareTo(entry2.getKey());
      if (compare < 0) {
        copyDocIdToRemoveDelete(
            des,
            entry1.getKey(),
            new LinkedList<>(this.getRemovedDocIdx()),
            this.readDocIds(entry1),
            0);

      } else if (compare > 0) {
        copyDocIdToRemoveDelete(
            des,
            entry2.getKey(),
            new LinkedList<>(inv.getRemovedDocIdx()),
            inv.readDocIds(entry2),
            0);
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
      }
    }
    while (it1.hasNext()) {

      entry1 = it1.next().getValue();
      copyDocIdToRemoveDelete(
          des,
          entry1.getKey(),
          new LinkedList<>(this.getRemovedDocIdx()),
          this.readDocIds(entry1),
          0);
    }
    while (it2.hasNext()) {
      entry2 = it2.next().getValue();
      copyDocIdToRemoveDelete(
          des,
          entry2.getKey(),
          new LinkedList<>(inv.getRemovedDocIdx()),
          this.readDocIds(entry2),
          ivL1Size);
    }

    des.flush();
    return des;
  }

  /**
   * @param indexFolder segment folder
   * @param indexName segment name use time stamp
   * @return InvertedIndex
   */
  public static InvertedIndex openInvertList(
      String indexFolder, String indexName, Integer headerLen) {

    InvertedIndex inv = new InvertedIndex(indexFolder, indexName, headerLen);
    inv.openReadOnlyDocDb();
    return inv;
  }

  // todo fix two many doc problem
  public void addDocument(Document document, Set<String> tokens) {
    this.documents.put(this.docNum, document);
    for (String key : tokens) {
      if (key.length() == 0 || key.length() > InvertedIndexHeaderEntry.keyByteSize) continue;
      if (this.invertList.containsKey(key)) {
        invertList.get(key).add(this.docNum);
      } else {
        invertList.put(key, new ArrayList<Integer>(Arrays.asList(this.docNum)));
      }
    }
    this.docNum++;
  }

  public void addDocument(Document document) {
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
    for (int i = 0; i < pageNum; i++) {
      ByteBuffer buffer = this.fileChannel.readPage(i);
      if (headerBuffer.position() > 0) {
        while (headerBuffer.remaining() > 0) {
          headerBuffer.put(buffer.get());
        }
        headerBuffer.flip();
        InvertedIndexHeaderEntry entry = new InvertedIndexHeaderEntry(headerBuffer);
        wordsDicEntries.put(entry.getKey(), entry);
        headerBuffer.clear();
        if (wordsDicEntries.size() >= this.headerNum) {
          return;
        }
      }

      while (buffer.remaining() >= InvertedIndexHeaderEntry.InvertedIndexHeaderEntrySize) {
        InvertedIndexHeaderEntry entry = new InvertedIndexHeaderEntry(buffer);
        wordsDicEntries.put(entry.getKey(), entry);
        if (wordsDicEntries.size() >= this.headerNum) {
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

  public Map<String, ArrayList<Integer>> searchWord(InvertedIndexHeaderEntry entry) {
    Preconditions.checkNotNull(wordsDicEntries);
    ArrayList<Integer> res = this.readDocIds(entry);
    Map<String, ArrayList<Integer>> map = new HashMap<>();
    map.put(entry.getKey(), res);
    return map;
  }

  public Map<String, ArrayList<Integer>> searchWords(ArrayList<String> words) throws Exception {
    //    System.out.println("Search words: " + Arrays.toString(words.toArray()));
    Map<String, ArrayList<Integer>> synchronizedMap = Collections.synchronizedMap(new HashMap<>());

    ExecutorService exec = Executors.newFixedThreadPool(4);

    for (String word : words) {
      if (this.wordsDicEntries.containsKey(word)) {
        final InvertedIndexHeaderEntry entry = this.wordsDicEntries.get(word);
        Runnable runnableTask =
            () -> {
              Map<String, ArrayList<Integer>> list = this.searchWord(entry);
              synchronized (synchronizedMap) {
                synchronizedMap.putAll(list);
              }
            };
        exec.execute(runnableTask);
      } else {
        synchronized (synchronizedMap) {
          synchronizedMap.put(word, new ArrayList<Integer>());
        }
      }
    }
    exec.shutdown();
    try {
      while (!exec.awaitTermination(1L, TimeUnit.HOURS)) {
        System.out.println("Not yet. Still waiting for termination");
      }

    } catch (InterruptedException e) {

    }
    return synchronizedMap;
  }

  private BitSet andOper(Map<String, ArrayList<Integer>> map) {
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

  private BitSet OrOper(Map<String, ArrayList<Integer>> map) {
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
      map.putAll(this.searchWords(words));
    } catch (Exception e) {
      return new HashMap<>();
    }

    if (map.size() == 0) return new HashMap<>();

    BitSet checker;
    if (query.equals("AND")) checker = andOper(map);
    else if (query.equals("OR")) {
      checker = OrOper(map);
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
      return this.readDocuments(docIdx);
    } catch (Exception e) {
      System.out.println("read docs idx: " + Arrays.toString(docIdx.toArray()) + " failed");
      return new HashMap<>();
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
    int pageStart = entry.getPtr() / PageFileChannel.PAGE_SIZE;
    int pageEnd =
        (entry.getPtr()
                + entry.getSize() * InvertedIndexHeaderEntry.listByteSize
                + PageFileChannel.PAGE_SIZE
                - 1)
            / PageFileChannel.PAGE_SIZE;
    int offset =
        pageStart > 0 ? entry.getPtr() - pageStart * PageFileChannel.PAGE_SIZE : entry.getPtr();

    ArrayList<Integer> docIds = new ArrayList<>();
    ByteBuffer intBuffer = ByteBuffer.allocate(InvertedIndexHeaderEntry.listByteSize);
    for (int i = pageStart; i <= pageEnd; i++) {
      ByteBuffer buffer = this.fileChannel.readPage(i);
      if (i == pageStart) {
        buffer.position(offset);
      }
      while (intBuffer.position() > 0) {
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
      while (buffer.remaining() >= InvertedIndexHeaderEntry.listByteSize) {
        docIds.add(buffer.getInt());
        if (docIds.size() == entry.getSize()) {
          return docIds;
        }
      }
    }
    return docIds;
  }

  public String getBasePath() {
    return basePath;
  }

  public LinkedList<Integer> getRemovedDocIdx() {
    return new LinkedList<Integer>(removedDocIdx);
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

    return this.invertList;
  }

  public Map<Integer, Document> getAllDocuments() {
    if (this.docStore == null) this.openReadOnlyDocDb();
    Map<Integer, Document> docs = new HashMap<>();
    for (int i = 0; i < (int) this.docStore.size(); i++) {
      if (!this.removedDocIdx.contains(i)) docs.put(i, this.docStore.getDocument(i));
    }
    return docs;
  }

  public ArrayList<Integer> deleteDocuments(String keyword) {
    ArrayList<String> keywords = new ArrayList<>();

    keywords.add(keyword);

    if (this.wordsDicEntries.isEmpty()) {
      this.readHeader();
    }
    try {
      Map<String, ArrayList<Integer>> wordsDocMap = this.searchWords(keywords);
      for (Map.Entry<String, ArrayList<Integer>> entry : wordsDocMap.entrySet()) {
        this.removedDocIdx.addAll(entry.getValue());
      }

    } catch (Exception e) {
      System.err.println("Delete keyword " + keyword + " errors!");
      return new ArrayList<>();
    }
    return new ArrayList<>(this.removedDocIdx);
  }
}
