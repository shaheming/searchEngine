package edu.uci.ics.cs221.index.inverted;

import com.google.common.base.Preconditions;
import edu.uci.ics.cs221.analysis.Analyzer;
import edu.uci.ics.cs221.storage.Document;
import edu.uci.ics.cs221.storage.DocumentStore;
import edu.uci.ics.cs221.storage.MapdbDocStore;

import java.io.IOException;
import java.io.*;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.List;
import java.util.*;

/**
 * This class manages an disk-based inverted index and all the documents in the inverted index.
 *
 * Please refer to the project 2 wiki page for implementation guidelines.
 */
public class InvertedIndexManager {
    private Analyzer analyzer;
    private String indexFolder;
    private Map<String,List<Integer>> invertlist=new TreeMap<>();
    private Map<Integer, Document> documents = new TreeMap<>();
    public DocumentStore dbDocStore;
    private PageFileChannel pageFileChannel;
    private String dbpath;
    private String name;
    private Path path;
    private int seg_counter;
    /**
     * The default flush threshold, in terms of number of documents.
     * For example, a new Segment should be automatically created whenever there's 1000 documents in the buffer.
     *
     * In test cases, the default flush threshold could possibly be set to any number.
     */
    public static int DEFAULT_FLUSH_THRESHOLD = 1000;
    /**
     * The default merge threshold, in terms of number of segments in the inverted index.
     * When the number of segments reaches the threshold, a merge should be automatically triggered.
     *
     * In test cases, the default merge threshold could possibly be set to any number.
     */
    public static int DEFAULT_MERGE_THRESHOLD = 8;

    private boolean checkAndCreateDir(String path) {
        File directory = new File(path);
        if (!directory.exists()) {
            directory.mkdirs();
            return false;
        }
        return true;
    }

    private InvertedIndexManager(String indexFolder, Analyzer analyzer) {
        this.analyzer=analyzer;
        this.indexFolder=indexFolder;
        checkAndCreateDir(indexFolder);
        dbpath=indexFolder+"/test.db";
        this.name="/segment"+getNumSegments();
        path=Paths.get(indexFolder+name+".txt");
        this.pageFileChannel=PageFileChannel.createOrOpen(path);
        seg_counter=0;


    }

    /**
     * Creates an inverted index manager with the folder and an analyzer
     */
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
     * Adds a document to the inverted index.
     * Document should live in a in-memory buffer until `flush()` is called to write the segment to disk.
     * @param document
     */
    public void addDocument(Document document) {
        if(dbDocStore==null)
            dbDocStore=MapdbDocStore.createWithBulkLoad(dbpath,documents.entrySet().iterator());
        List<String> temp=analyzer.analyze(document.getText());
        int total=(int)dbDocStore.size(); //global id
        dbDocStore.addDocument(total,document);//
        int n=temp.size();
        for(int i=0;i<n;i++) {
           if(!invertlist.containsKey(temp.get(i))){
               List<Integer> list=new ArrayList<Integer>();
               list.add(total);
               invertlist.put(temp.get(i),list);
           }
           else{
               invertlist.get(temp.get(i)).add(total);
           }
        }
        int after_insert_size=invertlist.size();
        if(after_insert_size>=DEFAULT_FLUSH_THRESHOLD){
            flush();
            invertlist.clear();
            seg_counter++;
            name="/segment"+getNumSegments();
            path=Paths.get(indexFolder+name+".txt");
            pageFileChannel=PageFileChannel.createOrOpen(path);

        }

        //throw new UnsupportedOperationException();
    }

    /**
     * Flushes all the documents in the in-memory segment buffer to disk. If the buffer is empty, it should not do anything.
     * flush() writes the segment to disk containing the posting list and the corresponding document store.
     */
    public void flush() {

        int flush_counter=0;
        Iterator iter=invertlist.entrySet().iterator();
        int n=invertlist.size()*48;//1000*48=48000
        int sum=0;
        while(iter.hasNext()){
            flush_counter++;
            ByteBuffer buffer_temp=ByteBuffer.allocate(48);
            Map.Entry <String, ArrayList<Integer>> entry=(Map.Entry )iter.next();
            int temp=entry.getValue().size();

            String str=entry.getKey();
            if(str.length()>20) str=str.substring(0,20);
            System.out.println("size: "+str.length()+" "+str+"temp :"+temp);
            char chars[]= str.toCharArray();
            for(int i=0;i<chars.length;i++){
                buffer_temp.putChar(chars[i]);
            }
            buffer_temp.putInt(temp);
            buffer_temp.putInt(n+sum);

            sum=sum+temp*4;
            pageFileChannel.appendAllBytes(buffer_temp);
            buffer_temp.clear();
        }

        if(getNumSegments()>DEFAULT_MERGE_THRESHOLD)
            mergeAllSegments();
        
    }





    /**
     * Merges all the disk segments of the inverted index pair-wise.
     */
    public void mergeAllSegments() {
        // merge only happens at even number of segments
        Preconditions.checkArgument(getNumSegments() % 2 == 0);
        int n=getNumSegments();



        //throw new UnsupportedOperationException();
    }

    /**
     * Performs a single keyword search on the inverted index.
     * You could assume the analyzer won't convert the keyword into multiple tokens.
     * If the keyword is empty, it should not return anything.
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
        int n=keywords.size();

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

    /**
     * Iterates through all the documents in all disk segments.
     */
    public Iterator<Document> documentIterator() {
        throw new UnsupportedOperationException();
    }

    /**
     * Deletes all documents in all disk segments of the inverted index that match the query.
     * @param keyword
     */
    public void deleteDocuments(String keyword) {
        throw new UnsupportedOperationException();
    }

    /**
     * Gets the total number of segments in the inverted index.
     * This function is used for checking correctness in test cases.
     *
     * @return number of index segments.
     */
    public int getNumSegments() {
        return seg_counter;
    }

    /**
     * Reads a disk segment into memory based on segmentNum.
     * This function is mainly used for checking correctness in test cases.
     *
     * @param segmentNum n-th segment in the inverted index (start from 0).
     * @return in-memory data structure with all contents in the index segment, null if segmentNum don't exist.
     */
    public InvertedIndexSegmentForTest getIndexSegment(int segmentNum) {
        throw new UnsupportedOperationException();
    }


}
