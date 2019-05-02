package edu.uci.ics.cs221;

import edu.uci.ics.cs221.analysis.Analyzer;
import edu.uci.ics.cs221.analysis.NaiveAnalyzer;
import edu.uci.ics.cs221.index.inverted.InvertedIndex;
import edu.uci.ics.cs221.search.FullScanSearcher;
import edu.uci.ics.cs221.storage.Document;
import edu.uci.ics.cs221.storage.DocumentStore;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

import static edu.uci.ics.cs221.storage.MapdbDocStore.createOrOpen;


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

    public InvertedIndexHeaderEntry(ByteBuffer buffer) {
        char[] strArray = new char[20];
        for (int i = 0; i < InvertedIndexHeaderEntry.keyByteSize; i++) {
            char c = (char) buffer.get();
            if (c == '\0') {
                this.wordkey = new String(Arrays.copyOfRange(strArray, 0, i));
                break;
            } else {
                strArray[i] = c;
            }
        }
        for (int i = this.wordkey.length() + 1; i < InvertedIndexHeaderEntry.keyByteSize; i++) {
            buffer.get();
        }

        this.size = buffer.getInt();
        this.ptr = buffer.getInt();
        System.out.println(this.wordkey);
        System.out.println(this.size);
        System.out.println(this.ptr);
    }

    public void writeToByteBuffer(ByteBuffer buffer) {

        for (int i = 0; i < this.wordkey.length(); i++) {
            buffer.put((byte) this.wordkey.charAt(i));
        }
        for (int i = this.wordkey.length(); i < keyByteSize; i++) {
            buffer.put((byte) 0);
        }
        buffer.putInt(this.size);
        buffer.putInt(this.ptr);
    }
}

class MyByteBuffer {
    private ByteBuffer buffer1 = ByteBuffer.allocate(20);
    private ByteBuffer buffer2 = ByteBuffer.allocate(20);
    private Integer limit = 10;
    private ByteBuffer buffer = buffer1;

    public MyByteBuffer() {
    }

    public MyByteBuffer putChar(char val) {
        checkFill();
        buffer.putChar(val);
        return this;
    }

    public MyByteBuffer putInt(int val) {
        checkFill();
        buffer.putInt(val);
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
        checkFill();
        buffer.put(b);
        return this;
    }

    private void checkFill() {
        if (buffer.position() >= this.limit) {
            System.out.println();
            Arrays.toString(buffer1.array());
            ByteBuffer bufferOld = buffer;
            if (buffer == buffer1) {
                buffer = buffer2;
            } else {
                buffer = buffer1;
            }
            bufferOld.flip();
            bufferOld.position(limit);
            Integer n = bufferOld.remaining();
            if (n > 0)
                buffer.put(bufferOld);

//            System.out.println(Arrays.toString(buffer1.array()));
//            System.out.println(Arrays.toString(buffer2.array()));
            bufferOld.clear();
        }
    }
}

/**
 * s
 * This is a Hello World program of our CS221 Peterman Search Engine.
 * It shows how to use the skeleton API of the search engine:
 * 1. use the provided DocumentStore to add and save documents
 * 2. use the provided FullScanSearcher and NaiveAnalyzer to do a search
 * <p>
 * Over this quarter, you will be implementing various analyzers, indexes, query types, and ranking
 * to make the search engine more efficient, powerful, and user-friendly :)
 */

public class HelloWorld {

    public static void main(String[] args) throws Exception {
        String docStorePath = "./docs.db";
        Files.deleteIfExists(Paths.get(docStorePath));

        // create the document store, add a few documents, and close() to flush them to disk
        DocumentStore documentStore = createOrOpen(docStorePath);

        documentStore.addDocument(0, new Document("UCI CS221 Information Retrieval"));
        documentStore.addDocument(1, new Document("Donald Bren School of Information and Computer Sciences"));
        documentStore.addDocument(2, new Document("UCI School of ICS "));

        documentStore.close();

        // open the existing document store again. use FullScanSearcher to search a query
        documentStore = createOrOpen(docStorePath);

        String query = "information";

        Analyzer analyzer = new NaiveAnalyzer();
        FullScanSearcher fullScanSearcher = new FullScanSearcher(documentStore, analyzer);
        List<Integer> searchResult = fullScanSearcher.search(query);

        System.out.println("query: " + query);
        System.out.println("search results: ");
        for (int docID : searchResult) {
            System.out.println(docID + ": " + documentStore.getDocument(docID));
        }

        fullScanSearcher.close();

        InvertedIndex invertedList = InvertedIndex.openInvertList("./index/Team3StressTest/", "20190501_21:51:15.864", 9352);
        invertedList.readHeader();
        System.out.println(invertedList.readDocIds(278));
        System.out.println(invertedList.readDocIds(279));
        System.out.println(invertedList.readDocIds(280));
        System.out.println(invertedList.readDocIds(332));

    }

}
