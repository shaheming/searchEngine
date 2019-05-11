package edu.uci.ics.cs221.index.inverted;

import com.google.common.base.Preconditions;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import com.sun.xml.internal.messaging.saaj.util.ByteOutputStream;
import edu.uci.ics.cs221.storage.Document;
import edu.uci.ics.cs221.storage.DocumentStore;
import edu.uci.ics.cs221.storage.MapdbDocStore;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Timestamp;
import java.util.AbstractMap.SimpleEntry;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
// class postingList
class LRUPageCache {
  private Integer capacity = 3;
  private Map<Integer, SimpleEntry<Integer, ByteBuffer>> map = new HashMap<>();
  private LinkedList<SimpleEntry<Integer, ByteBuffer>> list = new LinkedList<>();

  LRUPageCache() {}

  LRUPageCache(Integer c) {
    this.capacity = c;
  }

  public void put(Integer page_num, ByteBuffer page_buffer) {
    if (list.size() > capacity) {
      SimpleEntry<Integer, ByteBuffer> entry = list.removeFirst();
      this.map.remove(entry.getKey());
    }
    SimpleEntry<Integer, ByteBuffer> entry = new SimpleEntry<>(page_num, page_buffer);
    list.add(entry);
    map.put(page_num, entry);
  }

  public ByteBuffer get(Integer page_num) {
    if (map.containsKey(page_num)) {
      SimpleEntry<Integer, ByteBuffer> entry = map.get(page_num);
      list.remove(entry);
      entry.getValue().clear();
      list.add(entry);
      return entry.getValue();
    } else return null;
  }

  public void clear() {
    this.map.clear();
    this.list.clear();
  }
}

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

  public WritePageBuffer put(ByteBuffer src) {
    while (src.hasRemaining()) {
      checkFull();
      buffer.put(src.get());
    }
    return this;
  }

  public WritePageBuffer put(byte b) {
    buffer.put(b);
    checkFull();
    return this;
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
  static Integer keyByteSize = 30;
  static Integer ptrByteSize = 4;
  static Integer docIdByteSize = 4;
  static Integer InvertedIndexHeaderEntrySize = ptrByteSize + ptrByteSize + docIdByteSize;
  private String key;
  private Integer size;
  private Integer docIdListPtr;
  private Integer keyPtr;

  public InvertedIndexHeaderEntry(String key, Integer size, Integer ptr) {
    this.key = key;
    this.size = size;
    this.docIdListPtr = ptr;
  }

  InvertedIndexHeaderEntry() {}

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

  public Integer getKeyPtr() {
    return keyPtr;
  }

  public void setKeyPtr(Integer keyPtr) {
    this.keyPtr = keyPtr;
  }

  public void getKeyFromHeader(byte[] words, Integer nextKeyPtr) {
    this.key =
        new String(Arrays.copyOfRange(words, this.keyPtr, nextKeyPtr), StandardCharsets.UTF_8);
  }

  public String getKey() {
    return key;
  }

  public void setKey(String key) {
    this.key = key;
  }

  public Integer getSize() {
    return size;
  }

  public void setSize(Integer size) {
    this.size = size;
  }

  void print() {
    System.out.println(this.key + " " + this.size + " " + this.docIdListPtr);
  }

  Integer getDocIdListPtr() {
    return docIdListPtr;
  }

  void setDocIdListPtr(Integer docIdListPtr) {
    this.docIdListPtr = docIdListPtr;
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
public class InvertedIndex implements AutoCloseable {
  protected Map<Integer, Document> documents = new HashMap<>();
  private Integer docNum = 0;
  private String workPath;
  private String invertListPath;
  private String invertListDir;
  private String docStorePath;
  private String docStoreDir;
  private String positionListPath;
  private Integer headerLen = 0; // Byte
  private DocumentStore docStore;
  private Map<String, List<Integer>> invertList = new TreeMap<>();
  private Map<String, List<Integer>> positionList = new TreeMap<>();
  private PageFileChannel invertedListFileChannel;
  private PageFileChannel positionListFileChannel;
  private String segmentName;
  private Integer headerNum = 0;
  private Map<String, InvertedIndexHeaderEntry> wordsDicEntries = new TreeMap<>();
  private Set<Integer> removedDocIdx = new TreeSet<>();
  private DeltaVarLenCompressor compressor = new DeltaVarLenCompressor();
  //  private NaiveCompressor compressor = new NaiveCompressor();
  private LRUPageCache postingListLRU = new LRUPageCache();
  private LRUPageCache positionListLRU = new LRUPageCache();
  private ByteOutputStream positListBuffer = null;

  /** @param workPath the path of work dir */
  public InvertedIndex(String workPath) {
    InitFilePath(workPath, getTimeStamp());
    this.checkAndCreateDir(this.invertListDir);
    this.checkAndCreateDir(this.docStoreDir);
  }

  private void InitFilePath(String dir, String name) {
    this.workPath = dir;
    this.invertListDir = this.workPath + "/";
    this.docStoreDir = this.workPath + "/";
    this.segmentName = name;
    this.invertListPath = this.invertListDir + this.segmentName + ".list";
    this.positionListPath = this.invertListDir + this.segmentName + ".plist";
    this.docStorePath = this.docStoreDir + this.segmentName + ".db";
  }

  private void checkAndCreateDir(String path) {
    File directory = new File(path);
    if (!directory.exists()) {
      directory.mkdirs();
    }
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
    if (this.invertedListFileChannel == null) {
      this.invertedListFileChannel = PageFileChannel.createOrOpen(Paths.get(this.invertListPath));
    }
    if (this.positionListFileChannel == null) {
      this.positionListFileChannel = PageFileChannel.createOrOpen(Paths.get(this.positionListPath));
    }

    if (!Files.exists(indexPath)) {
      throw new RuntimeException(indexPath + " do not find");

    } else if (!Files.exists(indexDocPath)) {
      throw new RuntimeException(indexDocPath + " do not find");
    }
    this.openReadOnlyDocDb();
  }

  private boolean openReadOnlyDocDb() {
    if (this.docStore != null) return false;
    else {
      this.docStore = MapdbDocStore.createOrOpenReadOnly(this.docStorePath);
      return true;
    }
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

  private void calHeaderLen() {
    this.headerLen =
        this.invertList.size() * (InvertedIndexHeaderEntry.InvertedIndexHeaderEntrySize);
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
          inv.readDocIds(entry2),
          ivL1Size);
      dq2.poll();
    }

    des.flush();
    this.close();
    inv.close();
    des.close();
    return des;
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
      des.addDocument(entry.getValue());
      counter++;
    }
    return counter;
  }

  /**
   * this method will add document
   *
   * @param document
   */
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
   * todo flush invertList to disk(create the invertList file) flush docStore to disk
   *
   * @return
   */
  public boolean flush() {
    if (this.invertList.isEmpty() && this.documents.isEmpty()) return false;

    if (this.invertedListFileChannel == null) {
      this.invertedListFileChannel = PageFileChannel.createOrOpen(Paths.get(this.invertListPath));
    }
    WritePageBuffer writeBuffer = new WritePageBuffer(this.invertedListFileChannel);

    ExecutorService exec = Executors.newFixedThreadPool(2);

    if (this.docStore == null) {
      Runnable storeDocument =
          () -> {
            this.docStore =
                MapdbDocStore.createWithBulkLoad(
                    this.docStorePath, this.documents.entrySet().iterator());
          };
      exec.execute(storeDocument);
    }

    Runnable storeInvertedList =
        () -> {
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
          byte[] wordsByteStream = wordsStr.getBytes();
          for (int i = 0; i < wordsByteStream.length; i++) {
            writeBuffer.put(wordsByteStream[i]);
          }

          headerLen =
              wordsByteStream.length
                  + invertList.size() * InvertedIndexHeaderEntry.InvertedIndexHeaderEntrySize;

          Integer curListPtr = headerLen;
          Integer curWordPtr = 0;

          InvertedIndexHeaderEntry headerEntry = new InvertedIndexHeaderEntry();
          ByteArrayOutputStream listbuffer = new ByteArrayOutputStream();
          ByteBuffer intBuff = ByteBuffer.allocate(4);
          for (Map.Entry<String, List<Integer>> entry : this.invertList.entrySet()) {
            headerEntry.setKey(entry.getKey());
            headerEntry.setSize(entry.getValue().size());
            headerEntry.setKeyPtr(curWordPtr);
            headerEntry.setDocIdListPtr(curListPtr);
            byte[] encoded = compressor.encode(entry.getValue());
            intBuff.putInt(encoded.length);
            listbuffer.write(intBuff.array(), 0, intBuff.capacity());
            intBuff.clear();
            listbuffer.write(encoded, 0, encoded.length);
            curListPtr += encoded.length + 4;
            curWordPtr += entry.getKey().getBytes().length;
            headerEntry.convertToByte(writeBuffer);
          }

          writeBuffer.put(ByteBuffer.wrap(listbuffer.toByteArray()));
          writeBuffer.flush();
        };

    exec.execute(storeInvertedList);
    if (this.positionListFileChannel == null) {
      this.positionListFileChannel = PageFileChannel.createOrOpen(Paths.get(this.positionListPath));
    }
    if (this.positListBuffer != null) {
      WritePageBuffer positionListwriteBuffer = new WritePageBuffer(this.positionListFileChannel);

      positionListwriteBuffer.put(
          ByteBuffer.wrap(this.positListBuffer.getBytes(), 0, this.positListBuffer.getCount()));
    }

    exec.shutdown();
    try {
      while (!exec.awaitTermination(1L, TimeUnit.HOURS)) {
        System.out.println("Not yet. Still waiting for termination");
      }
    } catch (Exception e) {
      System.out.println(e.toString());
    }
    System.out.println("Write segment: " + this.segmentName + " docNum: " + this.docStore.size());

    this.close();
    return true;
  }

  @Override
  public void close() {
    if (this.invertedListFileChannel != null) {
      this.invertedListFileChannel.close();
    }
    if (this.docStore != null) {
      this.docStore.close();
    }
    if (this.positionListFileChannel != null) {
      this.positionListFileChannel.close();
    }
  }

  /** deserialize the header */
  public void readHeader() {

    Integer pageNum = (this.headerLen + PageFileChannel.PAGE_SIZE - 1) / PageFileChannel.PAGE_SIZE;
    ByteBuffer headerBuffer =
        ByteBuffer.allocate(InvertedIndexHeaderEntry.InvertedIndexHeaderEntrySize);

    Integer wordsLen =
        this.headerLen - this.headerNum * InvertedIndexHeaderEntry.InvertedIndexHeaderEntrySize;

    byte[] wordsbuffer = new byte[wordsLen];
    InvertedIndexHeaderEntry lastEntry = null;
    Integer counter = 0;
    for (int i = 0; i < pageNum; i++) {
      ByteBuffer buffer = this.readPostingListPage(i);
      // read header words
      if (counter < wordsLen) {
        while (buffer.hasRemaining()) {
          wordsbuffer[counter] = buffer.get();
          counter++;
          if (counter.equals(wordsLen)) break;
        }
        if (counter < wordsLen) continue;
        else {
          //          words = new String(wordsbuffer, StandardCharsets.UTF_8);
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
          lastEntry.getKeyFromHeader(wordsbuffer, entry.getKeyPtr());
          wordsDicEntries.put(lastEntry.getKey(), lastEntry);
        }

        lastEntry = entry;
        headerBuffer.clear();
        if (wordsDicEntries.size() == this.headerNum - 1) {
          lastEntry.getKeyFromHeader(wordsbuffer, wordsLen);
          wordsDicEntries.put(lastEntry.getKey(), lastEntry);
          return;
        }
      }

      while (buffer.remaining() >= InvertedIndexHeaderEntry.InvertedIndexHeaderEntrySize) {
        InvertedIndexHeaderEntry entry = new InvertedIndexHeaderEntry(buffer);
        if (lastEntry != null) {
          lastEntry.getKeyFromHeader(wordsbuffer, entry.getKeyPtr());
          wordsDicEntries.put(lastEntry.getKey(), lastEntry);
        }
        lastEntry = entry;
        if (wordsDicEntries.size() == this.headerNum - 1) {
          lastEntry.getKeyFromHeader(wordsbuffer, wordsLen);
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

  // this method provide a LRU buffered read for the posting list page read.
  private ByteBuffer readPostingListPage(int pageStart) {
    if (this.invertedListFileChannel == null) {
      this.invertedListFileChannel = PageFileChannel.createOrOpen(Paths.get(this.invertListPath));
    }
    ByteBuffer buffer = postingListLRU.get(pageStart);
    if (buffer == null) {
      buffer = this.invertedListFileChannel.readPage(pageStart);
      postingListLRU.put(pageStart, buffer);
    }
    return buffer;
  }

  private ArrayList<Integer> readDocIds(InvertedIndexHeaderEntry entry) {
    int pageStart = getPtrPageNum(entry.getDocIdListPtr());
    int offset = getPtrOffset(pageStart, entry.getDocIdListPtr());

    ArrayList<Integer> docIds = new ArrayList<>();
    PostiListPageIterator pit = new PostiListPageIterator(pageStart);
    ByteBuffer buffer = pit.next();
    buffer.position(offset);

    SimpleEntry<Integer, ByteBuffer> out = getListLen(buffer, pit);
    buffer = out.getValue();
    int counter = out.getKey();

    ByteOutputStream bytesteam = readDocIdsFromPage(buffer, pit, counter);
    if (bytesteam.getCount() > 0) {
      List<Integer> idx = compressor.decode(bytesteam.getBytes(), 0, bytesteam.getCount());
      docIds.addAll(idx);
    }
    //    System.out.println(entry.getKey() + Arrays.toString(docIds.toArray()));

    return docIds;
  }

  static Integer getPtrPageNum(Integer ptr) {
    return ptr / PageFileChannel.PAGE_SIZE;
  }

  static Integer getPtrOffset(Integer pageNum, Integer ptr) {
    return pageNum > 0 ? ptr - pageNum * PageFileChannel.PAGE_SIZE : ptr;
  }

  private ByteOutputStream readDocIdsFromPage(
      ByteBuffer buffer, Iterator<ByteBuffer> pit, Integer counter) {
    ByteOutputStream bytesteam = new ByteOutputStream();
    while (counter > 0) {
      int len =
          buffer.capacity() - buffer.position() > counter
              ? counter
              : buffer.capacity() - buffer.position();
      bytesteam.write(buffer.array(), buffer.position(), len);
      counter -= len;
      if (counter <= 0) break;
      buffer = pit.next();
    }
    return bytesteam;
  }

  private SimpleEntry<Integer, ByteBuffer> getListLen(ByteBuffer buffer, Iterator<ByteBuffer> pit) {
    int counter = 0;
    ByteBuffer intBuffer = ByteBuffer.allocate(InvertedIndexHeaderEntry.docIdByteSize);
    while (true) {
      if (intBuffer.position() > 0) {
        while (intBuffer.hasRemaining()) {
          intBuffer.put(buffer.get());
        }
        intBuffer.flip();
        counter = intBuffer.getInt();
        intBuffer.clear();
        break;
      }
      while (buffer.hasRemaining() && intBuffer.hasRemaining()) {
        intBuffer.put(buffer.get());
      }
      if (!intBuffer.hasRemaining()) {
        intBuffer.flip();
        counter = intBuffer.getInt();
        intBuffer.clear();
        break;
      }
      buffer = pit.next();
    }
    return new SimpleEntry<Integer, ByteBuffer>(counter, buffer);
  }

  private LinkedList<Integer> getRemovedDocIdx() {
    return new LinkedList<>(removedDocIdx);
  }

  public InvertedIndex setRemovedDocIdx(ArrayList<Integer> removedList) {
    this.removedDocIdx = new TreeSet<>(removedList);
    return this;
  }

  private ByteBuffer readPositionListPage(int pageStart) {
    if (this.positionListFileChannel == null) {
      this.positionListFileChannel = PageFileChannel.createOrOpen(Paths.get(this.positionListPath));
    }
    ByteBuffer buffer = positionListLRU.get(pageStart);
    if (buffer == null) {
      buffer = this.invertedListFileChannel.readPage(pageStart);
      positionListLRU.put(pageStart, buffer);
    }
    return buffer;
  }

  /**
   * add document and token to the invertedlist this will filter the invalid keywords, which exceed
   * the MAX length of keyword
   *
   * @param document
   * @param positionMap
   */
  public void addDocument(Document document, Map<String, ArrayList<Integer>> positionMap) {
    this.documents.put(this.docNum, document);
    if (this.positListBuffer == null) {
      this.positListBuffer = new ByteOutputStream(PageFileChannel.PAGE_SIZE * 2);
    }
    ByteBuffer intBuffer = ByteBuffer.allocate(4);
    for (String key : positionMap.keySet()) {
      // if (key.length() == 0 || key.length() > InvertedIndexHeaderEntry.keyByteSize)
      // continue;
      // convert position list to bytearray
      int positionListPtr = this.positListBuffer.getCount();
      ArrayList<Integer> positionList = positionMap.get(key);
      byte b[] = this.compressor.encode(positionList);
      intBuffer.putInt(b.length);
      this.positListBuffer.write(intBuffer.array());
      this.positListBuffer.write(b);
      intBuffer.clear();

      // write position list pointer to inverted list
      if (this.positionList.containsKey(key)) {
        this.positionList.get(key).add(positionListPtr);
      } else {
        ArrayList<Integer> pList = new ArrayList<>();
        pList.add(positionListPtr);
        this.positionList.put(key, pList);
      }

      if (this.invertList.containsKey(key)) {
        invertList.get(key).add(this.docNum);
      } else {
        invertList.put(key, new ArrayList<>(Arrays.asList(this.docNum)));
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

  /**
   * parallel reading the document indexes from the file
   *
   * @param words
   * @return
   */
  public Map<String, ArrayList<Integer>> readWordsDocIdx(ArrayList<String> words) {
    // System.out.println("Search words: " + Arrays.toString(words.toArray()));
    Map<String, ArrayList<Integer>> synchronizedMap = Collections.synchronizedMap(new HashMap<>());

    ExecutorService exec = Executors.newFixedThreadPool(2);

    for (String word : words) {
      if (this.wordsDicEntries.containsKey(word)) {
        final InvertedIndexHeaderEntry entry = this.wordsDicEntries.get(word);
        synchronizedMap.put(entry.getKey(), this.readDocIds(entry));
      } else {
        //        synchronized (synchronizedMap) {
        //          synchronizedMap.put(word, new ArrayList<>());
        //        }
        synchronizedMap.put(word, new ArrayList<>());
      }
    }
    //    exec.shutdown();
    //    try {
    //      while (!exec.awaitTermination(1L, TimeUnit.MINUTES)) {
    //        System.out.println("Not yet. Still waiting for termination");
    //      }
    //
    //    } catch (InterruptedException e) {
    //
    //    }
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

  public int getDocNumFromDb() {
    if (this.docStore == null) throw new RuntimeException("Db is not been open");
    return (int) this.docStore.size();
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

  public Map<String, Document> readDocuments(ArrayList<Integer> idex) throws InterruptedException {
    this.openReadOnlyDocDb();
    Map<String, Document> synchronizedMap = Collections.synchronizedMap(new HashMap<>());

    ExecutorService exec = Executors.newFixedThreadPool(2);
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

  public Map<String, List<Integer>> getAllInvertList() {
    if (this.wordsDicEntries.size() == 0) {
      this.readHeader();
    }
    for (InvertedIndexHeaderEntry entry : this.wordsDicEntries.values()) {
      this.invertList.put(entry.getKey(), this.readDocIds(entry));
    }
    this.invertedListFileChannel.close();
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

  /** @return */
  public Table<String, Integer, List<Integer>> getAllpositionList() {
    Preconditions.checkArgument(this.invertList.size() != 0);
    Preconditions.checkArgument(this.wordsDicEntries.size() != 0);
    Table<String, Integer, List<Integer>> table = HashBasedTable.create();
    for (Map.Entry<String, List<Integer>> entry : this.invertList.entrySet()) {
      InvertedIndexHeaderEntry headerEntry = this.wordsDicEntries.get(entry.getKey());
      List<Integer> positionPtrs = this.readPositionListPtrs(headerEntry);
      List<Integer> docIds = entry.getValue();
      for (int i = 0; i < docIds.size(); i++) {
        List<Integer> positionList = this.readPositionList(positionPtrs.get(i));
        table.put(entry.getKey(), docIds.get(i), positionList);
      }
    }
    return table;
  }

  private ArrayList<Integer> readPositionList(Integer ptr) {
    int startPageNum = getPtrPageNum(ptr);
    int offset = getPtrOffset(startPageNum, ptr);
    ArrayList<Integer> positonList = new ArrayList<>();
    PositionListPageIterator<ByteBuffer> pit = new PositionListPageIterator<>(startPageNum);
    ByteBuffer buffer = pit.next();
    buffer.position(offset);

    SimpleEntry<Integer, ByteBuffer> out = getListLen(buffer, pit);
    buffer = out.getValue();
    int counter = out.getKey();

    ByteOutputStream bytesteam = readDocIdsFromPage(buffer, pit, counter);
    if (bytesteam.getCount() > 0) {
      List<Integer> ptrs = compressor.decode(bytesteam.getBytes(), 0, bytesteam.getCount());
      positonList.addAll(ptrs);
    }

    return positonList;
  }

  private ArrayList<Integer> readPositionListPtrs(InvertedIndexHeaderEntry entry) {
    int pageStart = getPtrPageNum(entry.getDocIdListPtr());
    int offset = getPtrOffset(pageStart, entry.getDocIdListPtr());

    ArrayList<Integer> positonPtrs = new ArrayList<>();
    PostiListPageIterator pit = new PostiListPageIterator(pageStart);
    ByteBuffer buffer = pit.next();
    buffer.position(offset);

    SimpleEntry<Integer, ByteBuffer> out = getListLen(buffer, pit);
    buffer = out.getValue();
    int counter = out.getKey();
    // skip the invert list;
    do {
      if (buffer.remaining() > counter) {
        buffer.position(buffer.position() + counter);
        break;
      } else {
        counter -= buffer.remaining();
        buffer = pit.next();
      }
    } while (pit.hasNext() || buffer.remaining() > 0);

    out = getListLen(buffer, pit);
    buffer = out.getValue();
    counter = out.getKey();

    ByteOutputStream bytesteam = readDocIdsFromPage(buffer, pit, counter);

    if (bytesteam.getCount() > 0) {
      List<Integer> ptrs = compressor.decode(bytesteam.getBytes(), 0, bytesteam.getCount());
      positonPtrs.addAll(ptrs);
    }
    return positonPtrs;
  }

  private class PostiListPageIterator<k> implements Iterator {

    int index;
    int max;

    PostiListPageIterator(Integer startPage) {
      index = startPage;
      if (invertedListFileChannel == null) {
        invertedListFileChannel = PageFileChannel.createOrOpen(Paths.get(invertListPath));
      }
      this.max = invertedListFileChannel.getNumPages();
    }

    @Override
    public ByteBuffer next() {
      if (this.hasNext()) {
        return readPostingListPage(index++);
      }
      return null;
    }

    @Override
    public boolean hasNext() {
      if (this.index < this.max) {
        return true;
      }
      return false;
    }
  }

  private class PositionListPageIterator<k> implements Iterator {

    int index;
    int max;

    PositionListPageIterator(Integer startPage) {
      index = startPage;
      if (positionListFileChannel == null) {
        positionListFileChannel = PageFileChannel.createOrOpen(Paths.get(positionListPath));
      }
      this.max = positionListFileChannel.getNumPages();
    }

    @Override
    public ByteBuffer next() {
      if (this.hasNext()) {
        return readPositionListPage(index++);
      }
      return null;
    }

    @Override
    public boolean hasNext() {
      if (this.index < this.max) {
        return true;
      }
      return false;
    }
  }
}
