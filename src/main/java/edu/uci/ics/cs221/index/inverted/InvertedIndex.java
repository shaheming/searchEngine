package edu.uci.ics.cs221.index.inverted;

import edu.uci.ics.cs221.storage.Document;
import edu.uci.ics.cs221.storage.DocumentStore;
import edu.uci.ics.cs221.storage.MapdbDocStore;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.sql.Timestamp;
import java.util.*;

import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;


class InvertedIndexHeaderEntry {
    public String wordkey;
    public Integer size;
    public Integer ptr;
    public static Integer keyByteSize = 20;
    public static Integer ptrByteSize = 4;
    public static Integer listByteSize = 4;
    public static Integer InvertedIndexHeaderEntrySize = keyByteSize + ptrByteSize + listByteSize;

    public InvertedIndexHeaderEntry(String key, Integer size, Integer ptr) {
        this.wordkey = key;
        this.size = size;
        this.ptr = ptr;
    }

    public InvertedIndexHeaderEntry(ByteBuffer buffer, Integer position) {
        char[] strArray = new char[20];
        for (int i = 0; i < InvertedIndexHeaderEntry.keyByteSize; i++) {
            char c = (char) buffer.get(position + i);
            if (c == '\0') {
                this.wordkey = new String(Arrays.copyOfRange(strArray, 0, i));
                break;
            } else {
                strArray[i] = c;
            }
        }

        this.size = buffer.getInt(InvertedIndexHeaderEntry.keyByteSize);
        this.ptr = buffer.getInt(InvertedIndexHeaderEntry.keyByteSize + InvertedIndexHeaderEntry.listByteSize);
//        System.out.println(this.wordkey);
//        System.out.println(this.size);
//        System.out.println(this.ptr);
    }

    public void writeToByteBuffer(ByteBuffer buffer, Integer offset) {

        for (int i = 0; i < this.wordkey.length(); i++) {
            buffer.put(offset + i, (byte) this.wordkey.charAt(i));
        }
        buffer.putInt(offset + this.keyByteSize, this.size);
        buffer.putInt(offset + this.keyByteSize + this.listByteSize, this.ptr);
    }
}

public class InvertedIndex {
    private Integer docNum = 0;
    private Map<String, ArrayList<Integer>> invertList = new TreeMap<String, ArrayList<Integer>>();
    private String invertListPath;
    private String invertListDir;
    private String docStorePath;
    private String docStoreDir;
    private Integer headerLen = 0; //Byte
    private DocumentStore docStore;
    private Map<Integer, Document> documents = new HashMap<>();
    FileChannel fileChannel;

    /**
     * @param dir
     */
    public InvertedIndex(String dir) {
        this.invertListDir = dir + "/index/";
        this.docStoreDir = dir + "/doc/";
        String fileName = (new Timestamp(System.currentTimeMillis()))
                .toString()
                .replace(" ", "_")
                .replace("-", "");
        this.invertListPath = this.invertListDir + fileName + ".list";
        this.docStorePath = this.docStoreDir + fileName + ".db";
        this.checkAndCreateDir(this.invertListDir);
        this.checkAndCreateDir(this.docStoreDir);

        createOrOpen(false);
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
        this.invertListPath = this.invertListDir + fileName;
        this.docStorePath = this.docStoreDir + fileName;

        Path indexPath = Paths.get(this.invertListPath);
        Path indexDocPath = Paths.get(this.docStorePath);
        if (!Files.exists(indexPath)) {
            throw new RuntimeException(indexPath + " do not find");

        } else if (!Files.exists(indexDocPath)) {
            throw new RuntimeException(indexDocPath + " do not find");
        } else {
            if (!readHeader(headerLen))
                throw new RuntimeException(indexPath + " can not read header");
        }
        createOrOpen(true);
    }

    private boolean createOrOpen(boolean isReadOnly) {
        try {
            if (isReadOnly) {
                this.docStore = MapdbDocStore.createOrOpenReadOnly(this.docStorePath);
                this.fileChannel = FileChannel.open(Paths.get(this.invertListPath), READ);
            } else {
                this.docStore = MapdbDocStore.createOrOpen(this.docStorePath);
                this.fileChannel = FileChannel.open(Paths.get(this.invertListPath), READ, WRITE, StandardOpenOption.CREATE_NEW);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return true;
    }

    /**
     * todo deserialize the header
     *
     * @param headerLen
     * @return
     */
    public boolean readHeader(Integer headerLen) {
        try {
            ByteBuffer buffer = ByteBuffer.allocate(headerLen);
            this.fileChannel.read(buffer, headerLen);
            this.headerLen = headerLen;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return true;
    }

    /**
     * todo flush invertList to disk(create the invertList file)
     * flush docStore to disk
     *
     * @return
     */
    public boolean flush() {
        this.docStore = MapdbDocStore.createWithBulkLoad(this.docStorePath, this.documents.entrySet().iterator());
        throw new UnsupportedOperationException();
    }

    //merge two invertList
    public boolean merge(InvertedIndex inv) {
        throw new UnsupportedOperationException();
//      return false;
    }


    /**
     * @param indexFolder
     * @param indexName
     * @return
     */

    public static InvertedIndex openInvertList(String indexFolder, String indexName, Integer headerLen) {
        try {
            Path indexFolderPath = Paths.get(indexFolder);
            if (Files.exists(indexFolderPath) && Files.isDirectory(indexFolderPath)) {
                if (Files.isDirectory(indexFolderPath)) {
                    return new InvertedIndex(indexFolder);
                } else {
                    throw new RuntimeException(indexFolderPath + " already exists and is not a directory");
                }
            } else {
                Files.createDirectories(indexFolderPath);
                return new InvertedIndex(indexFolder);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public void addDocument(Document document, Set<String> tokens) {
        this.documents.put(docNum, document);
        Iterator<String> it = tokens.iterator();
        while (it.hasNext()) {
            String key = it.next();
            if (this.invertList.containsKey(key)) {
                ArrayList<Integer> list = invertList.get(key);
                list.add(docNum);
            } else {
                invertList.put(key, new ArrayList<Integer>(Arrays.asList(docNum)));
            }
        }
        docNum++;
//        for(Map.Entry<String,ArrayList<Integer>>entry:this.invertList.entrySet()){
//
//        }
    }

    public Integer getDocNum() {
        return docNum;
    }


}
