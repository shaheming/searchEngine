package edu.uci.ics.cs221.index.inverted;

import edu.uci.ics.cs221.storage.Document;
import edu.uci.ics.cs221.storage.DocumentStore;
import edu.uci.ics.cs221.storage.MapdbDocStore;

import java.io.File;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Timestamp;
import java.util.*;

class MyByteBuffer {
    private ByteBuffer buffer1 = ByteBuffer.allocate(PageFileChannel.PAGE_SIZE + 100);
    private ByteBuffer buffer2 = ByteBuffer.allocate(PageFileChannel.PAGE_SIZE + 100);
    private Integer limit = PageFileChannel.PAGE_SIZE;
    private ByteBuffer buffer = buffer1;
    private PageFileChannel fileChannel;

    public MyByteBuffer(PageFileChannel fileChannel) {
        this.fileChannel = fileChannel;

    }

    public MyByteBuffer putChar(char val) {
        checkFill();
        buffer.putChar(val);
        return this;
    }

    public MyByteBuffer putInt(int val) {
        buffer.putInt(val);
        checkFill();
        return this;
    }

    public MyByteBuffer put(ByteBuffer src) {
        int n = src.remaining();
        for (int i = 0; i < n; i++) {
            if (buffer.position() >= this.limit) {
                checkFill();
            }
            buffer.put(src.get());
        }
        return this;
    }

    public MyByteBuffer put(byte b) {
        buffer.put(b);
        checkFill();
        return this;
    }

    private void checkFill() {
        if (buffer.position() >= this.limit) {
            System.out.println("in new page");
            Arrays.toString(buffer1.array());
            ByteBuffer bufferOld = buffer;
            if (buffer == buffer1) {
                buffer = buffer2;
            } else {
                buffer = buffer1;
            }

            bufferOld.limit(bufferOld.position());
            bufferOld.position(limit);
            Integer n = bufferOld.remaining();
            if (n > 0)
                buffer.put(bufferOld);
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
    public static Integer keyByteSize = 20;
    public static Integer ptrByteSize = 4;
    public static Integer listByteSize = 4;
    public static Integer InvertedIndexHeaderEntrySize = keyByteSize + ptrByteSize + listByteSize;

    public InvertedIndexHeaderEntry(String key, Integer size, Integer ptr) {
        this.key = key;
        this.size = size;
        this.ptr = ptr;
    }

    public InvertedIndexHeaderEntry() {
    }

    public void setKey(String key) {
        this.key = key;
    }

    public void setSize(Integer size) {
        this.size = size;
    }

    public void setPtr(Integer ptr) {
        this.ptr = ptr;
    }

    public String getKey() {
        return key;
    }

    public Integer getSize() {
        return size;
    }

    public Integer getPtr() {
        return ptr;
    }

    public InvertedIndexHeaderEntry(ByteBuffer buffer) {
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
//        this.print();
    }

    public void print() {
        System.out.println(this.key + " " + this.size + " " + this.ptr);
    }

    public void writeToMyByteBuffer(MyByteBuffer buffer) {

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
    protected Integer docNum = 0;
    protected Map<String, ArrayList<Integer>> invertList = new TreeMap<String, ArrayList<Integer>>();
    protected String invertListPath;
    protected String invertListDir;
    protected String docStorePath;
    protected String docStoreDir;
    protected Integer headerLen = 0; //Byte
    protected DocumentStore docStore;
    protected Map<Integer, Document> documents = new HashMap<>();
    protected PageFileChannel fileChannel;
    protected String basePath;
    private String segmentName;
    protected Integer headerNum = 0;
    private MyByteBuffer mybuffer;
    private ArrayList<InvertedIndexHeaderEntry> wordsDicEntries = new ArrayList<>();

    public String getBasePath() {
        return basePath;
    }

    /**
     * @param dir
     */
    public InvertedIndex(String dir) {
        this.basePath = dir;
        this.invertListDir = this.basePath + "/index/";
        this.docStoreDir = this.basePath + "/doc/";
        this.segmentName = (new Timestamp(System.currentTimeMillis()))
                .toString()
                .replace(" ", "_")
                .replace("-", "");
        this.invertListPath = this.invertListDir + this.segmentName + ".list";
        this.docStorePath = this.docStoreDir + this.segmentName + ".db";
        this.checkAndCreateDir(this.invertListDir);
        this.checkAndCreateDir(this.docStoreDir);

        if (this.fileChannel == null) {
            this.fileChannel = PageFileChannel.createOrOpen(Paths.get(this.invertListPath));
        }
        this.mybuffer = new MyByteBuffer(this.fileChannel);
    }

    private void checkAndCreateDir(String path) {
        File directory = new File(path);
        if (!directory.exists()) {
            directory.mkdirs();
        }
    }

    /**
     * @param dir
     */
    public InvertedIndex(String dir, String fileName, Integer headerLen) {
        this.invertListDir = dir + "/index/";
        this.docStoreDir = dir + "/doc/";
        this.invertListPath = this.invertListDir + fileName + ".list";
        this.docStorePath = this.docStoreDir + fileName + ".db";
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
        if (this.fileChannel == null) {
            this.fileChannel = PageFileChannel.createOrOpen(Paths.get(this.invertListPath));
        }
        this.mybuffer = new MyByteBuffer(this.fileChannel);
    }

    private boolean createOrOpen(boolean isReadOnly) {
        try {
            if (isReadOnly) {
//                this.docStore = MapdbDocStore.createOrOpenReadOnly(this.docStorePath);
                this.fileChannel = PageFileChannel.createOrOpen(Paths.get(this.invertListPath));
            } else {
//                this.docStore = MapdbDocStore.createOrOpen(this.docStorePath);
                this.fileChannel = PageFileChannel.createOrOpen(Paths.get(this.invertListPath));
            }
        } catch (UncheckedIOException e) {
            throw e;
        }
        return true;
    }


//    public boolean readHeader(Integer headerLen) {
//        try {
//            ByteBuffer buffer = this.fileChannel.readPage((headerLen + PageFileChannel.PAGE_SIZE - 1) / PageFileChannel.PAGE_SIZE);
////            this.fileChannel.read(buffer, headerLen);
//            this.headerLen = headerLen;
//            buffer.position(headerLen);
//            buffer.flip();
//        } catch (UncheckedIOException e) {
//            throw e;
//        }
//        return true;
//    }

    private void calHeaderLen() {
        this.headerLen = this.invertList.size() * (InvertedIndexHeaderEntry.InvertedIndexHeaderEntrySize);
    }

    /**
     * todo flush invertList to disk(create the invertList file)
     * flush docStore to disk
     *
     * @return
     */
    public boolean flush() {
        this.docStore = MapdbDocStore.createWithBulkLoad(this.docStorePath, this.documents.entrySet().iterator());
        //1.cal head size
        //2.write head
        //3.write list
        this.calHeaderLen();
        Integer curListPtr = this.headerLen;
        InvertedIndexHeaderEntry headerEntry = new InvertedIndexHeaderEntry();
        for (Map.Entry<String, ArrayList<Integer>> entry : this.invertList.entrySet()) {
            headerEntry.setKey(entry.getKey());
            headerEntry.setSize(entry.getValue().size());
            headerEntry.setPtr(curListPtr);
            curListPtr += entry.getValue().size() * 4;
            headerEntry.writeToMyByteBuffer(this.mybuffer);
        }
        for (Map.Entry<String, ArrayList<Integer>> entry : this.invertList.entrySet()) {
            for (Integer i : entry.getValue()) {
                mybuffer.putInt(i);
            }
        }
        mybuffer.flush();
        return true;

    }

    //merge two invertList
    public boolean merge(InvertedIndex inv) {
        throw new UnsupportedOperationException();
//      return false;
    }


    /**
     * @param indexFolder segment folder
     * @param indexName   segment name use time stamp
     * @return InvertedIndex
     */

    public static InvertedIndex openInvertList(String indexFolder, String indexName, Integer headerLen) {
        return new InvertedIndex(indexFolder, indexName, headerLen);

    }

    public void addDocument(Document document, Set<String> tokens) {
        this.documents.put(this.docNum, document);
        Iterator<String> it = tokens.iterator();
        while (it.hasNext()) {
            String key = it.next();
            if (this.invertList.containsKey(key)) {
                invertList.get(key).add(this.docNum);
            } else {
                invertList.put(key, new ArrayList<Integer>(Arrays.asList(this.docNum)));
            }
        }
        this.docNum++;

//        for(Map.Entry<String,ArrayList<Integer>>entry:this.invertList.entrySet()){
//
//        }
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

    /**
     * deserialize the header
     */
    public void readHeader() {
        Integer pageNum = (this.headerLen + PageFileChannel.PAGE_SIZE - 1) / PageFileChannel.PAGE_SIZE;
        ByteBuffer headerBuffer = ByteBuffer.allocate(InvertedIndexHeaderEntry.InvertedIndexHeaderEntrySize);
        for (int i = 0; i < pageNum; i++) {
            ByteBuffer buffer = this.fileChannel.readPage(i);
            if (headerBuffer.position() > 0) {
                while (headerBuffer.remaining() > 0) {
                    headerBuffer.put(buffer.get());
                }
                headerBuffer.flip();
                InvertedIndexHeaderEntry entry = new InvertedIndexHeaderEntry(headerBuffer);
                wordsDicEntries.add(entry);
                headerBuffer.clear();
                if (wordsDicEntries.size() >= this.headerNum) {
                    return;
                }

            }

            while (buffer.remaining() > InvertedIndexHeaderEntry.InvertedIndexHeaderEntrySize) {
                InvertedIndexHeaderEntry entry = new InvertedIndexHeaderEntry(buffer);
                wordsDicEntries.add(entry);
                if (wordsDicEntries.size() >= this.headerNum) {
                    return;
                }
            }
//            System.out.println("headerCounter: " + wordsDicEntries.size());
            if (buffer.remaining() > 0) {
                headerBuffer.put(buffer);
            }

        }

    }

    public ArrayList<Integer> readDocIds(int docI) {
        InvertedIndexHeaderEntry entry = this.wordsDicEntries.get(docI);
        int pageStart = entry.getPtr() / PageFileChannel.PAGE_SIZE;
        int pageEnd = (entry.getPtr() + entry.getSize() * InvertedIndexHeaderEntry.listByteSize + PageFileChannel.PAGE_SIZE - 1) / PageFileChannel.PAGE_SIZE;
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
                if (docIds.size() >= entry.getSize()) {
                    return docIds;
                }
            }
            while (buffer.remaining() > InvertedIndexHeaderEntry.listByteSize) {
                docIds.add(buffer.getInt());
                if (docIds.size() >= entry.getSize()) {
                    return docIds;
                }
            }
        }
        return docIds;
    }

}
