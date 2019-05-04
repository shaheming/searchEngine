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
    public  DocumentStore dbDocStore;
    private List<InvertedIndexSegmentForTest> seg_list=new ArrayList<>();
    private Map<Integer, Document> documents=new TreeMap<>();
    private PageFileChannel pageFileChannel;
    private String dbpath;
    private int seg_counter=0;

    public static int DEFAULT_FLUSH_THRESHOLD = 1000;
    public static int DEFAULT_MERGE_THRESHOLD = 8;

    private InvertedIndexManager(String indexFolder, Analyzer analyzer) {
        this.analyzer=analyzer;
        this.indexFolder=indexFolder;
        checkAndCreateDir(indexFolder);



    }

    private boolean checkAndCreateDir(String path) {
        File directory = new File(path);
        if (!directory.exists()) {
            directory.mkdirs();
            return false;
        }
        return true;
    }

    private Path set_path_segment(int i){
        Path path;
        path=Paths.get("/seg+"+i+"/"+"segment"+i+".txt");
        return path;
    }

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

    public void addDocument(Document document) {
        List<String> temp=analyzer.analyze(document.getText());
        int total=documents.size(); //
        documents.put(total,document);
        int after_insert_size=documents.size();
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

        if(after_insert_size>=DEFAULT_FLUSH_THRESHOLD){
            dbpath=indexFolder+"/test"+getNumSegments()+".db";
            dbDocStore = MapdbDocStore.createWithBulkLoad(dbpath, documents.entrySet().iterator());
            InvertedIndexSegmentForTest seg_test=new InvertedIndexSegmentForTest(invertlist,documents);
            seg_list.add(seg_test);
            flush();
            seg_counter++;
            documents = new TreeMap<>();
            invertlist=new TreeMap<>();

            if(getNumSegments()>DEFAULT_MERGE_THRESHOLD&&getNumSegments()%2==0)
                mergeAllSegments();
            dbDocStore.close();

        }
    }

    public void flush() {
        pageFileChannel=PageFileChannel.createOrOpen(set_path_segment(seg_counter));
        Iterator iter=invertlist.entrySet().iterator();
        int n=invertlist.size()*48;//1000*48=48000
        int sum=0;
        while(iter.hasNext()){
            ByteBuffer buffer_temp=ByteBuffer.allocate(48);
            Map.Entry <String, ArrayList<Integer>> entry=(Map.Entry)iter.next();
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
        pageFileChannel.close();
    }

    public void mergeAllSegments() {
        // merge only happens at even number of segments
        Preconditions.checkArgument(getNumSegments() % 2 == 0);
        int n=getNumSegments();
        for(int i=1;i<n;i=i+2) {
            PageFileChannel temp1 = PageFileChannel.createOrOpen(set_path_segment(i));
            PageFileChannel temp2 = PageFileChannel.createOrOpen(set_path_segment(i+1));
            ByteBuffer b1=temp1.readAllPages();
            ByteBuffer b2=temp2.readAllPages();


            temp1.close();
            temp2.close();
            File f1 = new File(indexFolder+"/seg"+i);
            File[] files1 = f1.listFiles();
            for (File file : files1) {
                file.delete();
            }
            f1.delete();

            File f2 = new File(indexFolder+"/seg"+i+1);
            File[] files2 = f2.listFiles();
            for (File file : files2) {
                file.delete();
            }
            f2.delete();





        }

        seg_counter=seg_counter/2;



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

    public InvertedIndexSegmentForTest getIndexSegment(int segmentNum) {
        if(seg_list.size()==0) return null;
        return seg_list.get(segmentNum);
    }


}
