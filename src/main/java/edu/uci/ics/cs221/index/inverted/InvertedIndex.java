package edu.uci.ics.cs221.index.inverted;

import com.google.common.base.Preconditions;
import com.google.common.collect.Table;
import com.google.common.collect.TreeBasedTable;
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
  private Integer capacity = 5;
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

class LRUPositionListCache {
  private Integer capacity = 5;
  private Map<Integer, SimpleEntry<Integer, ArrayList<Integer>>> map = new HashMap<>();
  private LinkedList<SimpleEntry<Integer, ArrayList<Integer>>> list = new LinkedList<>();

  LRUPositionListCache() {}

  LRUPositionListCache(Integer c) {
    this.capacity = c;
  }

  public void put(Integer ptr, ArrayList<Integer> positionList) {
    if (list.size() > capacity) {
      SimpleEntry<Integer, ArrayList<Integer>> entry = list.removeFirst();
      this.map.remove(entry.getKey());
    }
    SimpleEntry<Integer, ArrayList<Integer>> entry = new SimpleEntry<>(ptr, positionList);
    list.add(entry);
    map.put(ptr, entry);
  }

  public ArrayList<Integer> get(Integer page_num) {
    if (map.containsKey(page_num)) {
      SimpleEntry<Integer, ArrayList<Integer>> entry = map.get(page_num);
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
/** Note the write buffer should class flush after finish all writing behaviors */
class PageWriteBuffer {
  private ByteBuffer buffer1 = ByteBuffer.allocate(PageFileChannel.PAGE_SIZE + 100);
  private ByteBuffer buffer2 = ByteBuffer.allocate(PageFileChannel.PAGE_SIZE + 100);
  private Integer limit = PageFileChannel.PAGE_SIZE;
  private ByteBuffer buffer = buffer1;
  private PageFileChannel fileChannel;
  private Integer writeCount = 0;

  /**
   * this buffer use two buffer to align the data to one page size. When the page buffer is filled,
   * it will be flush to disk automatically
   *
   * @param fileChannel pagechennel
   */
  PageWriteBuffer(PageFileChannel fileChannel) {
    this.fileChannel = fileChannel;
  }

  public PageWriteBuffer putInt(int val) {
    buffer.putInt(val);
    checkFull();
    writeCount += 4;
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

  public PageWriteBuffer put(ByteBuffer src) {
    int inBytes = src.remaining();
    while (src.hasRemaining()) {
      checkFull();
      buffer.put(src.get());
    }
    writeCount += inBytes;
    return this;
  }

  // This is used to record how many bytes are written to the disk
  Integer getWriteCount() {
    return writeCount;
  }

  public PageWriteBuffer put(byte b) {
    buffer.put(b);
    writeCount += 1;
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

  InvertedIndexHeaderEntry(Integer keyPtr, Integer size, Integer docIdListPtr) {
    this.keyPtr = keyPtr;
    this.size = size;
    this.docIdListPtr = docIdListPtr;
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
  void convertToByte(PageWriteBuffer buffer) {
    buffer.putInt(this.keyPtr);
    buffer.putInt(this.size);
    buffer.putInt(this.docIdListPtr);
  }
}

class PageReadBuffer implements AutoCloseable {
  private PageFileChannel pageChannel;
  private LRUPageCache LRUCache;
  private String filePath;
  private PageIterator pit;
  private ByteBuffer pageBuffer;

  PageReadBuffer(String filePath) {
    this.filePath = filePath;
    LRUCache = new LRUPageCache();
  }

  public Boolean isOpen() {
    return this.pageChannel != null;
  }

  public void openChannel() {
    if (this.pageChannel != null) {
      try {
        this.pageChannel.close();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    this.pageChannel = PageFileChannel.createOrOpen(Paths.get(filePath));
  }

  public void setPosition(Integer ptr) {
    int startPage = ptr / PageFileChannel.PAGE_SIZE;
    int offset = startPage > 0 ? ptr - startPage * PageFileChannel.PAGE_SIZE : ptr;
    if (pit == null) pit = new PageIterator<>(startPage);
    else pit.setStartPage(startPage);
    pageBuffer = pit.next();
    pageBuffer.position(offset);
  }

  public void movePtrNBytes(Integer N) {
    if (!this.pit.hasNext() && this.pageBuffer.remaining() < N) {
      throw new RuntimeException("Out of range");
    }
    do {
      if (pageBuffer.remaining() > N) {
        pageBuffer.position(pageBuffer.position() + N);
        break;
      } else {
        N -= pageBuffer.remaining();
        pageBuffer = pit.next();
      }
    } while (pit != null && (pit.hasNext() || pageBuffer.remaining() > 0));
  }

  public Integer readListLen() {
    Preconditions.checkNotNull(this.pit);
    Preconditions.checkNotNull(this.pageBuffer);
    int counter;
    ByteBuffer intBuffer = ByteBuffer.allocate(InvertedIndexHeaderEntry.docIdByteSize);
    while (true) {
      if (intBuffer.position() > 0) {
        while (intBuffer.hasRemaining()) {
          intBuffer.put(this.pageBuffer.get());
        }
        intBuffer.flip();
        counter = intBuffer.getInt();
        intBuffer.clear();
        break;
      }
      while (this.pageBuffer.hasRemaining() && intBuffer.hasRemaining()) {
        intBuffer.put(this.pageBuffer.get());
      }
      if (!intBuffer.hasRemaining()) {
        intBuffer.flip();
        counter = intBuffer.getInt();
        intBuffer.clear();
        break;
      }
      this.pageBuffer = pit.next();
    }
    return counter;
  }

  public ByteOutputStream readNBytes(Integer n) {
    ByteOutputStream byteSteam = new ByteOutputStream();
    while (n > 0) {
      int len =
          this.pageBuffer.capacity() - pageBuffer.position() > n
              ? n
              : pageBuffer.capacity() - pageBuffer.position();
      byteSteam.write(pageBuffer.array(), pageBuffer.position(), len);
      pageBuffer.position(pageBuffer.position() + len);
      n -= len;
      if (n <= 0) break;
      pageBuffer = pit.next();
    }
    return byteSteam;
  }

  private ByteBuffer readPage(int pageNum) {
    ByteBuffer buffer = LRUCache.get(pageNum);
    if (buffer == null) {
      buffer = this.pageChannel.readPage(pageNum);
      LRUCache.put(pageNum, buffer);
    }
    return buffer;
  }

  public Integer getInt() {
    if (this.pageBuffer.remaining() > 4) {
      return this.pageBuffer.getInt();
    } else {
      ByteBuffer intBuffer = ByteBuffer.allocate(4);
      Preconditions.checkState(this.pit.hasNext(), "Page out of range");
      while (this.pageBuffer.hasRemaining()) {
        intBuffer.put(this.pageBuffer.get());
      }
      this.pageBuffer = this.pit.next();
      while (intBuffer.hasRemaining()) {
        intBuffer.put(this.pageBuffer.get());
      }
      intBuffer.flip();
      return intBuffer.getInt();
    }
  }

  @Override
  public void close() {
    if (this.pageChannel != null) this.pageChannel.close();
  }

  private class PageIterator<k> implements Iterator {

    int index;
    int max;

    PageIterator(Integer startPage) {
      index = startPage;
      this.max = pageChannel.getNumPages();
    }

    public void setStartPage(Integer i) {
      Preconditions.checkState(i < max);
      this.index = i;
    }

    @Override
    public ByteBuffer next() {
      if (this.hasNext()) {
        return readPage(index++);
      }
      return null;
    }

    @Override
    public boolean hasNext() {
      return this.index < this.max;
    }
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
  private Compressor compressor = new DeltaVarLenCompressor();
  private PageReadBuffer postingListReadBuffer = null;
  private PageReadBuffer positionListReadBuffer = null;
  private LRUPositionListCache positionListCache = null;
  private Table<String, Integer, List<Integer>> position = TreeBasedTable.create();
  private Integer searchOrder = 0;
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

  public InvertedIndex(String workPath, Compressor compressor) {
    this.compressor = compressor;
    InitFilePath(workPath, getTimeStamp());
    this.checkAndCreateDir(this.invertListDir);
    this.checkAndCreateDir(this.docStoreDir);
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
    this.postingListReadBuffer = new PageReadBuffer(this.invertListPath);
    this.positionListReadBuffer = new PageReadBuffer(this.positionListPath);
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

  public InvertedIndex setSearchOrder(int order) {
    this.searchOrder = order;
    return this;
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
    InvertedIndex des = new InvertedIndex(this.workPath, this.compressor);
    PageWriteBuffer positionListWriteBuffer = des.openPositionListWriteBuffer();
    this.openReadOnlyDocDb();
    inv.openReadOnlyDocDb();
    // db1
    Integer ivL1Size = copyDocToRemoveDelete(this, des, new LinkedList<>(this.getRemovedDocIdx()));
    Integer ivL2Size = copyDocToRemoveDelete(inv, des, new LinkedList<>(inv.getRemovedDocIdx()));

    // invertList
    this.readHeader();
    inv.readHeader();
    this.postingListReadBuffer.openChannel();
    this.positionListReadBuffer.openChannel();
    inv.postingListReadBuffer.openChannel();
    inv.positionListReadBuffer.openChannel();

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
        copyDocIdToRemoveDelete(des, this, entry1, positionListWriteBuffer, 0);
        dq1.poll();

      } else if (compare > 0) {
        copyDocIdToRemoveDelete(des, inv, entry2, positionListWriteBuffer, ivL1Size);
        dq2.poll();
      } else {
        int offset = copyDocIdToRemoveDelete(des, this, entry1, positionListWriteBuffer, 0);
        copyDocIdToRemoveDelete(des, inv, entry2, positionListWriteBuffer, ivL1Size);
        dq1.poll();
        dq2.poll();
      }
    }
    while (!dq1.isEmpty()) {
      entry1 = dq1.peek().getValue();
      copyDocIdToRemoveDelete(des, this, entry1, positionListWriteBuffer, 0);
      dq1.poll();
    }
    while (!dq2.isEmpty()) {
      entry2 = dq2.peek().getValue();
      copyDocIdToRemoveDelete(des, inv, entry2, positionListWriteBuffer, ivL1Size);
      dq2.poll();
    }

    positionListWriteBuffer.flush();
    des.flush();
    this.close();
    inv.close();
    des.close();
    return des;
  }

  /**
   * This method use to filter the deleted documentIds. As for the position list, this method will
   * read each position list related each key and write it to disk immediately through
   * positionListWriteBuffer, and update the pointer.
   *
   * @param des
   * @param src
   * @param headerEntry
   * @param positionListWriteBuffer
   * @param offset
   * @return
   */
  private static Integer copyDocIdToRemoveDelete(
      InvertedIndex des,
      InvertedIndex src,
      InvertedIndexHeaderEntry headerEntry,
      PageWriteBuffer positionListWriteBuffer,
      Integer offset) {
    int counter = 0;
    Deque<Integer> removedDq = new LinkedList<>(src.getRemovedDocIdx());
    ArrayList<Integer> ids = src.readDocIds(headerEntry);
    ArrayList<Integer> ptrs = src.readPositionListPtrs(headerEntry);
    ArrayList<Integer> newDocIds = new ArrayList<>();
    ArrayList<Integer> newPtrs = new ArrayList<>();
    String key = headerEntry.getKey();

    // des.positListBuffer = new
    int newPositionListPtr = positionListWriteBuffer.getWriteCount();
    Iterator<Integer> it = ptrs.iterator();
    for (Integer id : ids) {
      int ptr = it.next();
      if ((removedDq.size() > 0) && removedDq.peek() <= id) {
        removedDq.poll();
        counter++;
        continue;
      }
      // write the position list to new list;
      ByteOutputStream rawBytes = src.readRawPositionList(ptr);
      positionListWriteBuffer.putInt(rawBytes.getCount());
      positionListWriteBuffer.put(ByteBuffer.wrap(rawBytes.getBytes(), 0, rawBytes.getCount()));
      newPtrs.add(newPositionListPtr);

      newPositionListPtr += rawBytes.getCount() + 4;

      newDocIds.add(id - counter + offset);
    }

    if (des.positionList.containsKey(key)) {
      des.positionList.get(key).addAll(newPtrs);
    } else {
      des.positionList.put(key, newPtrs);
    }

    if (des.invertList.containsKey(key)) {
      des.invertList.get(key).addAll(newDocIds);
      return des.invertList.get(key).size();
    } else {
      if (!newDocIds.isEmpty()) des.invertList.put(key, newDocIds);
      return 0;
    }
  }

  private ArrayList<Integer> readDocIds(InvertedIndexHeaderEntry entry) {
    if (!this.postingListReadBuffer.isOpen()) {
      this.postingListReadBuffer.openChannel();
    }
    this.postingListReadBuffer.setPosition(entry.getDocIdListPtr());
    int counter = this.postingListReadBuffer.readListLen();
    ByteOutputStream bytesteam = this.postingListReadBuffer.readNBytes(counter);
    List<Integer> idx = compressor.decode(bytesteam.getBytes(), 0, bytesteam.getCount());

    //    System.out.println(entry.getKey() + Arrays.toString(docIds.toArray()));

    return new ArrayList<>(idx);
  }

  private ArrayList<Integer> readPositionListPtrs(InvertedIndexHeaderEntry entry) {
    if (!this.postingListReadBuffer.isOpen()) {
      this.postingListReadBuffer.openChannel();
    }

    this.postingListReadBuffer.setPosition(entry.getDocIdListPtr());
    int counter = this.postingListReadBuffer.readListLen();
    this.postingListReadBuffer.movePtrNBytes(counter);
    // skip the invert list;

    counter = this.postingListReadBuffer.readListLen();

    ByteOutputStream byteSteam = this.postingListReadBuffer.readNBytes(counter);
    List<Integer> ptrs = compressor.decode(byteSteam.getBytes(), 0, byteSteam.getCount());
    return new ArrayList<>(ptrs);
  }

  private ByteOutputStream readRawPositionList(Integer ptr) {
    if (!this.positionListReadBuffer.isOpen()) {
      this.positionListReadBuffer.openChannel();
    }
    this.positionListReadBuffer.setPosition(ptr);
    int counter = this.positionListReadBuffer.readListLen();
    return this.positionListReadBuffer.readNBytes(counter);
  }

  private LinkedList<Integer> getRemovedDocIdx() {
    return new LinkedList<>(removedDocIdx);
  }

  public InvertedIndex setRemovedDocIdx(ArrayList<Integer> removedList) {
    this.removedDocIdx = new TreeSet<>(removedList);
    return this;
  }

  public PageWriteBuffer openPositionListWriteBuffer() {
    this.positionListFileChannel = PageFileChannel.createOrOpen(Paths.get(this.positionListPath));
    return new PageWriteBuffer(this.positionListFileChannel);
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
    PageWriteBuffer writeBuffer = new PageWriteBuffer(this.invertedListFileChannel);

    ExecutorService exec = Executors.newFixedThreadPool(1);

    if (this.docStore == null) {
      Runnable storeDocument =
          () ->
              this.docStore =
                  MapdbDocStore.createWithBulkLoad(
                      this.docStorePath, this.documents.entrySet().iterator());
      exec.execute(storeDocument);
    }
    exec.shutdown();

    // 1.cal head size
    // 2.write head
    // 3.write list
    StringBuilder builder = new StringBuilder();

    for (String s : this.invertList.keySet()) {
      builder.append(s);
    }
    // write words dic to file
    String wordsStr = builder.toString();

    byte[] wordsByteStream = wordsStr.getBytes();
    for (byte b : wordsByteStream) {
      writeBuffer.put(b);
    }

    headerLen =
        wordsByteStream.length
            + invertList.size() * InvertedIndexHeaderEntry.InvertedIndexHeaderEntrySize;
    PageWriteBuffer positionListWriteBuffer = null;

    if (this.position != null) {
      if (this.positionListFileChannel == null) {
        this.positionListFileChannel =
            PageFileChannel.createOrOpen(Paths.get(this.positionListPath));
      }
      positionListWriteBuffer = new PageWriteBuffer(this.positionListFileChannel);
    }

    Integer curListPtr = headerLen;
    int curWordPtr = 0;
    int curPositionListPtr = 0;
    InvertedIndexHeaderEntry headerEntry = new InvertedIndexHeaderEntry();
    ByteArrayOutputStream listStreamBuffer = new ByteArrayOutputStream();
    ByteBuffer intBuff = ByteBuffer.allocate(4);
    for (Map.Entry<String, List<Integer>> entry : this.invertList.entrySet()) {
      headerEntry.setKey(entry.getKey());
      headerEntry.setSize(entry.getValue().size());
      headerEntry.setKeyPtr(curWordPtr);
      headerEntry.setDocIdListPtr(curListPtr);
      byte[] encoded = compressor.encode(entry.getValue());
      intBuff.putInt(encoded.length);
      listStreamBuffer.write(intBuff.array(), 0, intBuff.capacity());
      intBuff.clear();
      listStreamBuffer.write(encoded, 0, encoded.length);
      curListPtr += encoded.length + 4;

      // this is used to merge flush
      if (!this.positionList.isEmpty()) {
        encoded = compressor.encode(this.positionList.get(entry.getKey()));
        intBuff.putInt(encoded.length);
        listStreamBuffer.write(intBuff.array(), 0, intBuff.capacity());
        intBuff.clear();
        listStreamBuffer.write(encoded, 0, encoded.length);
        curListPtr += encoded.length + 4;
      }

      // this is used to normal flush
      if (!this.position.isEmpty()) {
        ArrayList<Integer> ptrs = new ArrayList<>(entry.getValue().size());
        List<Integer> docIds = entry.getValue();
        for (Integer docId : docIds) {
          ptrs.add(curPositionListPtr);
          encoded = compressor.encode(this.position.get(entry.getKey(), docId));
          assert positionListWriteBuffer != null;
          positionListWriteBuffer.putInt(encoded.length);
          positionListWriteBuffer.put(ByteBuffer.wrap(encoded, 0, encoded.length));
          curPositionListPtr = curPositionListPtr + 4 + encoded.length;
        }
        intBuff.clear();
        encoded = compressor.encode(ptrs);
        intBuff.putInt(encoded.length);
        intBuff.clear();
        listStreamBuffer.write(intBuff.array(), 0, intBuff.capacity());
        intBuff.clear();
        listStreamBuffer.write(encoded, 0, encoded.length);
        curListPtr += encoded.length + 4;
      }

      curWordPtr += entry.getKey().getBytes().length;
      headerEntry.convertToByte(writeBuffer);
    }

    writeBuffer.put(ByteBuffer.wrap(listStreamBuffer.toByteArray()));
    writeBuffer.flush();
    assert positionListWriteBuffer != null;
    positionListWriteBuffer.flush();

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
    if (this.positionListReadBuffer != null) this.positionListReadBuffer.close();
    if (this.postingListReadBuffer != null) this.postingListReadBuffer.close();
  }

  /** deserialize the header */
  public void readHeader() {

    Integer wordsLen;
    wordsLen =
        this.headerLen - this.headerNum * InvertedIndexHeaderEntry.InvertedIndexHeaderEntrySize;

    InvertedIndexHeaderEntry lastEntry;

    if (!this.postingListReadBuffer.isOpen()) {
      this.postingListReadBuffer.openChannel();
    }
    this.postingListReadBuffer.setPosition(0);
    byte[] wordsBuffer = this.postingListReadBuffer.readNBytes(wordsLen).getBytes();
    lastEntry =
        new InvertedIndexHeaderEntry(
            this.postingListReadBuffer.getInt(),
            this.postingListReadBuffer.getInt(),
            this.postingListReadBuffer.getInt());

    for (int i = 1; i < this.headerNum; i++) {
      InvertedIndexHeaderEntry entry =
          new InvertedIndexHeaderEntry(
              this.postingListReadBuffer.getInt(),
              this.postingListReadBuffer.getInt(),
              this.postingListReadBuffer.getInt());
      lastEntry.getKeyFromHeader(wordsBuffer, entry.getKeyPtr());
      wordsDicEntries.put(lastEntry.getKey(), lastEntry);
      lastEntry = entry;
    }
    lastEntry.getKeyFromHeader(wordsBuffer, wordsLen);
    wordsDicEntries.put(lastEntry.getKey(), lastEntry);

    this.headerNum = this.wordsDicEntries.size();
  }

  public int getDocumentFrequency(String token) {
    if (this.wordsDicEntries.isEmpty()) this.readHeader();
//    for(Map.Entry<String, InvertedIndexHeaderEntry> entry : wordsDicEntries.entrySet()){
//      System.out.println(entry.getKey());
//    }
    if (this.wordsDicEntries.containsKey(token)) return this.wordsDicEntries.get(token).getSize();
    else return 0;
  }

  /**
   * Map<DocID, Double> dotProductAccumulator; <SegmentID, LocalDocID> Map <DocID, Double>
   * vectorLengthAccumulator;
   *
   * <p>for each segment: for each query token w: for each docID on the postingList of w: tfidf =
   * TF(w, docID) * IDF(w); dotProductAccumulator[docID] += tfidf * queryTfIdf[w];
   * vectorLengthAccumulator[docID] += tfidf ^ 2;
   *
   * <p>for each docID in this segment score(docID) = dotProductAccumulator[docID] /
   * sqrt(vectorLengthAccumulator[docID]);
   *
   * @param queryWordsTFIDF
   * @param topK
   * @return
   */
  public ArrayList<Pair<Double, Integer>> searchTfIdf(
      Map<String, Double> queryWordsTFIDF, Map<String, Double> globalWordsIDF, int topK) {

    if (this.wordsDicEntries.isEmpty()) {
      this.readHeader();
    }
    double[] dotProductAccumulator = new double[this.docNum];
    double[] vectorLengthAccumulator = new double[this.docNum];

    for (Map.Entry<String, Double> entry : queryWordsTFIDF.entrySet()) {
      if (!this.wordsDicEntries.containsKey(entry.getKey())) continue;
      InvertedIndexHeaderEntry invEntry = this.wordsDicEntries.get(entry.getKey());
      List<Integer> docIdx = this.readDocIds(invEntry);
      List<Integer> positionPtrs = this.readPositionListPtrs(invEntry);
      for (int i = 0; i < docIdx.size(); i++) {
        int docId = docIdx.get(i);

        /*tfidf = TF(w, docID) * IDF(w);*/
        Double tfidf =
            this.getPositionListSize(positionPtrs.get(i)) * globalWordsIDF.get(entry.getKey());
        dotProductAccumulator[docId] += tfidf * queryWordsTFIDF.get(entry.getKey());
        vectorLengthAccumulator[docId] += Math.pow(tfidf, 2);
      }
    }
    PriorityQueue<Pair<Double, Integer>> pq;
    pq = new PriorityQueue<>(topK * 2, Comparator.comparing(Pair::getLeft));
    for (int docId = 0; docId < this.docNum; docId++) {
      if (vectorLengthAccumulator[docId] > 0) {
        pq.add(
            new Pair<>(
                dotProductAccumulator[docId] / Math.sqrt(vectorLengthAccumulator[docId]), docId));
      }
      while (pq.size() > topK) pq.poll();
    }
    return new ArrayList<>(pq);
  }

  // get the word frequency in the document
  private Integer getPositionListSize(Integer ptr) {
    if (!this.positionListReadBuffer.isOpen()) {
      this.positionListReadBuffer.openChannel();
    }
    this.positionListReadBuffer.setPosition(ptr);
    return this.positionListReadBuffer.readListLen();
  }

  public void accumulateKeywordsTf(Map<String, Integer> wordsTf) {
    if (this.wordsDicEntries.size() == 0) this.readHeader();
    for (String word : wordsTf.keySet()) {
      if (this.wordsDicEntries.containsKey(word)) {
        wordsTf.put(word, wordsTf.get(word) + this.wordsDicEntries.get(word).getSize());
      }
    }
  }

  private int getDocNumFromDb() {
    if (this.docStore == null) throw new RuntimeException("Db is not been open");
    return (int) this.docStore.size();
  }

  public Integer getDocNum() {
    return this.docNum;
  }

  public InvertedIndex setDocNum(Integer docNum) {
    this.docNum = docNum;
    return this;
  }

  private void deleteFile() {}

  /**
   * add document and token to the invertedlist this will filter the invalid keywords, which exceed
   * the MAX length of keyword for a new inverted list, the position list will be stored in the
   * memory in the member this.position
   *
   * @param document
   * @param positionMap
   */
  public void addDocument(Document document, Map<String, ArrayList<Integer>> positionMap) {
    this.documents.put(this.docNum, document);

    for (String key : positionMap.keySet()) {

      ArrayList<Integer> positionList = positionMap.get(key);
      this.position.put(key, this.docNum, positionList);

      if (this.invertList.containsKey(key)) {
        invertList.get(key).add(this.docNum);
      } else {
        invertList.put(key, new ArrayList<>(Arrays.asList(this.docNum)));
      }
    }
    this.docNum++;
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
    ArrayList<String> filteredWords = new ArrayList<>();
    for (String word : words) {
      if (this.wordsDicEntries.containsKey(word)) {
        filteredWords.add(word);
      }
    }
    if (filteredWords.size() == 0) return new HashMap<>();

    BitSet checker;
    if (query.equals("AND")) checker = andAllIndex(words);
    else if (query.equals("OR")) {
      checker = orAllIndex(words);
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
    } finally {
      this.close();
    }
  }

  /**
   * do or operation to all inverted list
   *
   * @param words query words
   * @return
   */
  private BitSet andAllIndex(ArrayList<String> words) {
    int size = getDocNumFromDb();
    BitSet checker = new BitSet(size);
    checker.set(0, size, true);

    for (String word : words) {
      List<Integer> docIdx = this.readWordDocIdx(word);
      //      if (docIdx.size() == 0) continue;
      BitSet tmpChecker = new BitSet(size);
      for (int idx : docIdx) {
        tmpChecker.set(idx);
      }
      checker.and(tmpChecker);
    }
    for (int i : this.removedDocIdx) {
      checker.set(i, false);
    }
    return checker;
  }

  /**
   * parallel reading the document indexes from the file
   *
   * @param word
   * @return
   */
  public ArrayList<Integer> readWordDocIdx(String word) {
    if (this.wordsDicEntries.containsKey(word)) {
      final InvertedIndexHeaderEntry entry = this.wordsDicEntries.get(word);
      return this.readDocIds(entry);
    } else {
      return new ArrayList<>();
    }
  }

  private BitSet orAllIndex(ArrayList<String> words) {
    int size = getDocNumFromDb();
    BitSet checker = new BitSet(size);
    for (String word : words) {
      List<Integer> docIdx = this.readWordDocIdx(word);
      //      if (docIdx.size() == 0) continue;
      BitSet tmpChecker = new BitSet(size);
      for (int idx : docIdx) {
        tmpChecker.set(idx);
      }
      checker.or(tmpChecker);
    }
    for (int i : this.removedDocIdx) {
      checker.set(i, false);
    }
    return checker;
  }

  public Map<String, Document> searchPhrase(List<String> words) {
    if (words.size() == 0) return new HashMap<>();
    // read header
    this.readHeader();
    Map<String, ArrayList<Integer>> map;
    Map<String, ArrayList<Integer>> map_positional_ptr;
    ArrayList<Integer> docIdx = new ArrayList<>();

    try {
      map = this.readWordsDocIdx(words);
      map_positional_ptr = this.readWords_positional(words);

    } catch (Exception e) {
      return new HashMap<>();
    }
    if (map.size() == 0 || map_positional_ptr.size() == 0) return new HashMap<>();

    ArrayList<Integer> temp_ilist = map.get(words.get(0));
    for (int i = 0; i < temp_ilist.size(); i++) {
      docIdx.add(temp_ilist.get(i));
    }
    if (words.size() == 1) {
      try {
        return this.readDocuments(docIdx);
      } catch (Exception e) {
        return new HashMap<>();
      }
    }
    ArrayList<Integer> ptr_list = map_positional_ptr.get(words.get(0));
    for (int num = 0; num < docIdx.size(); num++) {
      if (docIdx.get(num) == -1) continue;
      boolean flag = true;
      Integer id = docIdx.get(num); // document id
      ArrayList<Integer> getlist = readPositionList(ptr_list.get(num)); // postional list
      for (int i = 0; i < getlist.size(); i++) {
        int counter = 0;
        int positional = getlist.get(i);
        for (int j = 1; j < words.size(); j++) {
          ArrayList<Integer> ptr_list_temp = map_positional_ptr.get(words.get(j));
          ArrayList<Integer> nextWordPositionList = map.get(words.get(j));
          int temp_index = Collections.binarySearch(nextWordPositionList, id);
          if (temp_index >= 0) {

            ArrayList<Integer> list_new = readPositionList(ptr_list_temp.get(temp_index));
            for (int l = 0; l < list_new.size(); l++) {
              if (list_new.get(l) == positional + j) {
                counter++;
                break;
              }
            }
          } else {
            flag = false;
            docIdx.set(num, -1);

            break;
          }
          if (!flag) break;
        }

        if (counter == words.size() - 1) {
          break;
        }
        if (i == getlist.size() - 1) {
          docIdx.set(num, -1);
          break;
        }

        if (!flag) break;
      }
    }

    ArrayList<Integer> final_list = new ArrayList<>();
    for (int i = 0; i < docIdx.size(); i++) {
      if (docIdx.get(i) != -1) final_list.add(docIdx.get(i));
    }

    try {
      return this.readDocuments(final_list);
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
  public Map<String, ArrayList<Integer>> readWordsDocIdx(List<String> words) {
    Map<String, ArrayList<Integer>> synchronizedMap = Collections.synchronizedMap(new HashMap<>());
    for (String word : words) {
      if (this.wordsDicEntries.containsKey(word)) {
        final InvertedIndexHeaderEntry entry = this.wordsDicEntries.get(word);
        synchronizedMap.put(entry.getKey(), this.readDocIds(entry));

      } else {
        synchronizedMap.put(word, new ArrayList<>());
      }
    }
    return synchronizedMap;
  }

  private Map<String, Document> readDocuments(ArrayList<Integer> idex) throws InterruptedException {
    this.openReadOnlyDocDb();
    Map<String, Document> synchronizedMap = Collections.synchronizedMap(new HashMap<>());

    ExecutorService exec = Executors.newFixedThreadPool(2);
    for (int id : idex) {
      Runnable runnableTask =
          () -> {
            Document doc = this.docStore.getDocument(id);
            synchronized (synchronizedMap) {
              synchronizedMap.put(
                  (char) this.searchOrder.intValue() + "id_" + id + "_" + this.segmentName, doc);
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

  private ArrayList<Integer> readPositionList(Integer ptr) {
    if (this.positionListCache == null) {
      positionListCache = new LRUPositionListCache();
    }
    ArrayList<Integer> positionList = positionListCache.get(ptr);
    if (positionList != null) {
      return positionList;
    }
    ByteOutputStream bytesteam = readRawPositionList(ptr);
    positionList = new ArrayList<>();
    if (bytesteam.getCount() > 0) {
      List<Integer> ptrs = compressor.decode(bytesteam.getBytes(), 0, bytesteam.getCount());
      positionList.addAll(ptrs);
    }
    positionListCache.put(ptr, positionList);
    return positionList;
  }

  public Map<String, ArrayList<Integer>> readWords_positional(List<String> words) {
    Map<String, ArrayList<Integer>> synchronizedMap = Collections.synchronizedMap(new HashMap<>());
    for (String word : words) {
      if (this.wordsDicEntries.containsKey(word)) {
        final InvertedIndexHeaderEntry entry = this.wordsDicEntries.get(word);
        synchronizedMap.put(entry.getKey(), this.readPositionListPtrs(entry));
      } else {
        synchronizedMap.put(word, new ArrayList<>());
      }
    }
    return synchronizedMap;
  }

  public Document readDocument(Integer docId) {
    this.openReadOnlyDocDb();
    return this.docStore.getDocument(docId);
  }

  public Map<String, List<Integer>> getAllInvertList() {
    if (this.wordsDicEntries.size() == 0) {
      this.readHeader();
    }
    for (InvertedIndexHeaderEntry entry : this.wordsDicEntries.values()) {
      this.invertList.put(entry.getKey(), this.readDocIds(entry));
    }
    //    this.invertedListFileChannel.close();
    return this.invertList;
  }

  public Map<Integer, Document> getAllDocuments() {
    if (this.docStore == null) this.openReadOnlyDocDb();
    Map<Integer, Document> docs = new HashMap<>();
    for (int i = 0; i < (int) this.docStore.size(); i++) {
      if (!this.removedDocIdx.contains(i)) docs.put(i, this.docStore.getDocument(i));
    }
    //    this.docStore.close();
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
    Table<String, Integer, List<Integer>> table = TreeBasedTable.create();
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

  public InvertedIndex setCompressor(Compressor compressor) {
    this.compressor = compressor;
    return this;
  }
}
