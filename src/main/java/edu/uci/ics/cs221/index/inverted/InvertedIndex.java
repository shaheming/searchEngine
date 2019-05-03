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
    checkFill();
    buffer.putChar(val);
    return this;
  }

  public WritePageBuffer putInt(int val) {
    checkFill();
    buffer.putInt(val);
    return this;
  }

  public WritePageBuffer put(ByteBuffer src) {
    int n = src.remaining();
    for (int i = 0; i < n; i++) {
      if (buffer.position() >= this.limit) {
        checkFill();
      }
      buffer.put(src.get());
    }
    return this;
  }

  public WritePageBuffer put(byte b) {
    buffer.put(b);
    checkFill();
    return this;
  }

  private void checkFill() {
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
      bufferOld.clear();
      this.flush(bufferOld);
    }
  }

  private void flush(ByteBuffer buffer) {
    buffer.position(PageFileChannel.PAGE_SIZE);
    buffer.flip();
    this.fileChannel.appendPage(buffer.slice());
    this.buffer.clear();
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
  static Integer keyByteSize = 20;
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
    char[] strArray = new char[20];
    for (int i = 0; i < InvertedIndexHeaderEntry.keyByteSize; i++) {
      char c = (char) buffer.get();
      if (c == '\0') {
        this.key = new String(Arrays.copyOfRange(strArray, 0, i));
        break;
      } else {
        strArray[i] = c;
      }
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
    for (int i = this.key.length(); i < keyByteSize; i++) {
      buffer.put((byte) 0);
    }
    buffer.putInt(this.size);
    buffer.putInt(this.ptr);
  }
}

public class InvertedIndex {
  private Integer docNum = 0;
  private Map<String, ArrayList<Integer>> invertList = new TreeMap<>();
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
  private ArrayList<Integer> removedDocIds = new ArrayList<>();

  public void setRemovedDocIds(ArrayList<Integer> removedList) {
    this.removedDocIds.addAll(removedList);
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

    if (this.fileChannel == null) {
      this.fileChannel = PageFileChannel.createOrOpen(Paths.get(this.invertListPath));
    }
    this.writeBuffer = new WritePageBuffer(this.fileChannel);
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

    if (this.docStore == null)
      this.docStore =
          MapdbDocStore.createWithBulkLoad(this.docStorePath, this.documents.entrySet().iterator());
    // 1.cal head size
    // 2.write head
    // 3.write list
    this.calHeaderLen();
    Integer curListPtr = this.headerLen;
    InvertedIndexHeaderEntry headerEntry = new InvertedIndexHeaderEntry();
    for (Map.Entry<String, ArrayList<Integer>> entry : this.invertList.entrySet()) {
      headerEntry.setKey(entry.getKey());
      headerEntry.setSize(entry.getValue().size());
      headerEntry.setPtr(curListPtr);
      curListPtr += entry.getValue().size() * 4;
      headerEntry.writeToMyByteBuffer(this.writeBuffer);
    }
    for (Map.Entry<String, ArrayList<Integer>> entry : this.invertList.entrySet()) {
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
    this.docStore = MapdbDocStore.createOrOpenReadOnly(this.docStorePath);
    return true;
  }

  public Iterator<Map.Entry<Integer, Document>> docIterator() {
    return this.docStore.iterator();
  }

  public void addInvertListItem(String key, Integer docId) {
    if (!this.invertList.containsKey(key)) {
      invertList.put(key, new ArrayList<>(Arrays.asList(docId)));
    } else {
      invertList.get(key).add(docId);
    }
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
    ArrayList<Integer> newDocIds = new ArrayList<Integer>();
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
    } else {
      des.invertList.put(key, newDocIds);
    }
    return des.invertList.get(key).size();
  }

  // todo cal doc size
  // todo merge second to the first
  public InvertedIndex merge(InvertedIndex inv) {
    InvertedIndex des = new InvertedIndex(this.basePath);
    this.openReadOnlyDocDb();
    inv.openReadOnlyDocDb();
    // db1
    Integer ivL1Size = copyDocToRemoveDelete(this, des, new LinkedList(this.getRemovedDocIds()));
    Integer ivL2Size = copyDocToRemoveDelete(inv, des, new LinkedList(inv.getRemovedDocIds()));

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
            new LinkedList(this.getRemovedDocIds()),
            this.readDocIds(entry1),
            0);

      } else if (compare > 0) {
        copyDocIdToRemoveDelete(
            des,
            entry2.getKey(),
            new LinkedList(inv.getRemovedDocIds()),
            inv.readDocIds(entry2),
            0);
      } else {
        int offset =
            copyDocIdToRemoveDelete(
                des,
                entry1.getKey(),
                new LinkedList(this.getRemovedDocIds()),
                this.readDocIds(entry1),
                0);
        copyDocIdToRemoveDelete(
            des,
            entry2.getKey(),
            new LinkedList(inv.getRemovedDocIds()),
            inv.readDocIds(entry2),
            ivL1Size);
      }
    }
    while (it1.hasNext()) {

      entry1 = it1.next().getValue();
      copyDocIdToRemoveDelete(
          des,
          entry1.getKey(),
          new LinkedList(this.getRemovedDocIds()),
          this.readDocIds(entry1),
          0);
    }
    while (it2.hasNext()) {
      entry2 = it2.next().getValue();
      copyDocIdToRemoveDelete(
          des,
          entry2.getKey(),
          new LinkedList(inv.getRemovedDocIds()),
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
    return new InvertedIndex(indexFolder, indexName, headerLen);
  }

  // todo fix two many doc problem
  public void addDocument(Document document, Set<String> tokens) {
    this.documents.put(this.docNum, document);
    for (String key : tokens) {
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

  public ArrayList<Integer> readDocIds(InvertedIndexHeaderEntry entry) {
    int pageStart = entry.getPtr() / PageFileChannel.PAGE_SIZE;
    int pageEnd =
        (entry.getPtr()
                + entry.getSize() * InvertedIndexHeaderEntry.listByteSize
                + PageFileChannel.PAGE_SIZE
                - 1)
            / PageFileChannel.PAGE_SIZE;
    int offset = entry.getPtr() - pageStart * PageFileChannel.PAGE_SIZE;
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

  public ArrayList<Integer> getRemovedDocIds() {
    return removedDocIds;
  }

  private String getTimeStamp() {
    return (new Timestamp(System.currentTimeMillis())).toString().replace(" ", "_").replace("-", "")
        + "_"
        + (int) (Math.random() * 1000 + 1) % 1000;
  }
}
