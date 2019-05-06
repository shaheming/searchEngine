package edu.uci.ics.cs221.index.inverted;

import com.google.common.base.Preconditions;
import edu.uci.ics.cs221.analysis.Analyzer;
import edu.uci.ics.cs221.storage.Document;
import edu.uci.ics.cs221.storage.DocumentStore;
import edu.uci.ics.cs221.storage.MapdbDocStore;

import java.io.IOException;
import java.io.*;
import java.io.UncheckedIOException;
import java.sql.Timestamp;
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
    private Map<Integer,List<Integer>> data=new HashMap<>();
    private Map<Integer, Document> documents=new TreeMap<>();
    private PageFileChannel pageFileChannel;
    private String dbpath;
    private String documenttext;
    private int seg_counter=0;
    private Map<Integer,Path> pathmap=new HashMap<>();

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

    private Path set_path_segment(String i){
        Path path;
        path=Paths.get(indexFolder+new Timestamp(System.currentTimeMillis()).toString()
                .replace(" ", "_")
                .replace("-", "")
                .replace(":", "")
                .replace(".", "")
                + "_"
                + (int) (Math.random() * 1000 + 1) % 1000+".txt");
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
        if(document.getText()=="") return;
        documenttext=document.getText();
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
            flush();
            if(getNumSegments()>DEFAULT_MERGE_THRESHOLD&&getNumSegments()%2==0)
                mergeAllSegments();
        }
    }

    public void flush() {
        if(documenttext==""||documents.size()==0||documents==null||invertlist.size()==0||invertlist==null) return;

        dbpath=indexFolder+"/test"+getNumSegments()+".db";
        dbDocStore = MapdbDocStore.createWithBulkLoad(dbpath, documents.entrySet().iterator());
        //InvertedIndexSegmentForTest seg_test=new InvertedIndexSegmentForTest(invertlist,documents);
        //seg_list.add(seg_test);
        dbDocStore.close();

        Path xx=set_path_segment(""+seg_counter);
        pageFileChannel=PageFileChannel.createOrOpen(xx);
        pathmap.put(seg_counter,xx);
        Iterator iter=invertlist.entrySet().iterator();
        Iterator iter1=invertlist.entrySet().iterator();
        int n=invertlist.size()*28;//1000*48=48000
        int sum=n;
        int counter=0;
        int num=PageFileChannel.PAGE_SIZE/28;//146 8
        List<Integer> word_length=new ArrayList<>();
        List<Integer> offset=new ArrayList<>();
        ByteBuffer buffer_temp=ByteBuffer.allocate(PageFileChannel.PAGE_SIZE);
        int buff_page=0;
        while(iter.hasNext()){
            Map.Entry <String, ArrayList<Integer>> entry=(Map.Entry)iter.next();
            int temp=entry.getValue().size();
            System.out.println("temp: "+temp);
            offset.add(temp);
            String str=entry.getKey();
            if(str.length()>20)
                str=str.substring(0,20);
            word_length.add(str.length());
            char chars[]= str.toCharArray();
            for(int i=0;i<chars.length;i++){
                if(buffer_temp.remaining()==0){
                    pageFileChannel.writePage(buff_page,buffer_temp);
                    buff_page++;
                    buffer_temp.clear();
                }
                buffer_temp.put((byte)chars[i]);

            }
            buffer_temp.position(20+counter*28);
            System.out.println("now p: "+ buffer_temp.position()+str);
            counter++;
            if(buffer_temp.remaining()<4){
                int remain = PageFileChannel.PAGE_SIZE-buffer_temp.position();
                ByteBuffer gg=ByteBuffer.allocate(4);
                gg.putInt(temp);
                gg.clear();
                for(int l=0;l<remain;l++){
                    buffer_temp.put(gg.get());
                }
                pageFileChannel.writePage(buff_page,buffer_temp);
                buff_page++;
                buffer_temp.clear();
                for(int l=0;l<4-remain;l++){
                    buffer_temp.put(gg.get());
                }
                gg.clear();
            }
            else buffer_temp.putInt(temp);

            if(buffer_temp.remaining()<4){
                int remain = PageFileChannel.PAGE_SIZE-buffer_temp.position();
                ByteBuffer gg=ByteBuffer.allocate(4);
                gg.putInt(sum);
                gg.clear();
                for(int l=0;l<remain;l++){
                    buffer_temp.put(gg.get());
                }
                pageFileChannel.writePage(buff_page,buffer_temp);
                buff_page++;
                buffer_temp.clear();
                for(int l=0;l<4-remain;l++){
                    buffer_temp.put(gg.get());
                }
                gg.clear();
            }
            else buffer_temp.putInt(sum);
            System.out.println("sum: "+sum);
            sum=sum+temp*4;

        }
        for(int i=0;i<invertlist.size();i++){

            Map.Entry <String, ArrayList<Integer>> entry=(Map.Entry)iter1.next();
            for(int j=0;j<offset.get(i);j++) {
                if(buffer_temp.remaining()<4){
                    System.out.println("ssssssssss");
                    int remain = PageFileChannel.PAGE_SIZE-buffer_temp.position();
                    ByteBuffer gg=ByteBuffer.allocate(4);
                    gg.putInt(entry.getValue().get(j));
                    gg.clear();
                    for(int l=0;l<remain;l++){
                        buffer_temp.put(gg.get());
                    }
                    pageFileChannel.writePage(buff_page,buffer_temp);
                    buff_page++;
                    buffer_temp.clear();
                    for(int l=0;l<4-remain;l++){
                        buffer_temp.put(gg.get());
                    }
                    gg.clear();
                }
                else buffer_temp.putInt(entry.getValue().get(j));
                System.out.println("innum: "+entry.getValue().get(j));
            }

        }


        pageFileChannel.appendAllBytes(buffer_temp);
        pageFileChannel.close();


        data.put(seg_counter,word_length);///store the size()
        seg_counter++;
        documents = new TreeMap<>();
        invertlist=new TreeMap<>();

    }

    public void mergeAllSegments() {
        // merge only happens at even number of segments
        Preconditions.checkArgument(getNumSegments() % 2 == 0);
        int n=getNumSegments();
        for(int i=1;i<n;i=i+2) {
            PageFileChannel temp1 = PageFileChannel.createOrOpen(set_path_segment(i+""));
            PageFileChannel temp2 = PageFileChannel.createOrOpen(set_path_segment(i+1+""));
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
        List<Document> list=new ArrayList<>();
        int n=getNumSegments();
        for(int i=0;i<n;i++){
            InvertedIndexSegmentForTest test=getIndexSegment(i);
            int doc_num=test.getDocuments().size();
            for(int j=0;j<doc_num;j++){
                


            }

        }

        Iterator<Document> iterator=list.iterator();
        return iterator;

       // throw new UnsupportedOperationException();
    }

    /**
     * Deletes all documents in all disk segments of the inverted index that match the query.
     * @param keyword
     */
    public void deleteDocuments(String keyword) {
        throw new UnsupportedOperationException();
    }
    public int getNumSegments() {
        return seg_counter;
    }

    private boolean checknextpage(ByteBuffer buffer){
        if(buffer.remaining()<4)
            return true;
        else return false;
    }

    public InvertedIndexSegmentForTest getIndexSegment(int segmentNum) {//start from 0
        if(data.isEmpty()||data==null) return null;
        Map<Integer, Document> docs=new TreeMap<>() ;
        Map<String,List<Integer>> plist=new TreeMap<>();
        dbpath=indexFolder+"/test"+segmentNum+".db";
        dbDocStore = MapdbDocStore.createOrOpen(dbpath);
        int docnum=(int)dbDocStore.size();
        for(int i=0;i<docnum;i++){
            Document temp=dbDocStore.getDocument(i);
            docs.put(i,temp);
        }
        dbDocStore.close();
        Path aa=pathmap.get(segmentNum);
        pageFileChannel=PageFileChannel.createOrOpen(aa);

        int n=data.get(segmentNum).size();//size
        List<Integer> list=data.get(segmentNum);
        for(int i=0;i<n;i++) System.out.println("real size: "+list.get(i));
        int page=0;
        ByteBuffer temp=pageFileChannel.readPage(0);
        temp.clear();
        for(int i=0;i<n;i++){
/////////////////
            char[] words=new char[list.get(i)];
            for(int j=0;j<list.get(i);j++){
                if(checknextpage(temp)){
                    page++;
                    temp=pageFileChannel.readPage(page);
                    System.out.println("next page");
                }
                byte uu=temp.get();
                words[j]=(char)uu;


            }
            String keyword= String.valueOf(words);
            System.out.println(temp.position()+" word: "+keyword+" number: "+list.get(i));
            int reminder=20-list.get(i);
            for(int j=0;j<reminder;j++) {
                if(temp.remaining()==0){
                    page++;
                    temp=pageFileChannel.readPage(page);
                    System.out.println("next page");
                }
                temp.get();
            }
/////////////////
            int length;
            if(checknextpage(temp)){

                int remain=temp.remaining();
                ByteBuffer hh=ByteBuffer.allocate(4);
                for(int l=0;l<remain;l++){
                    hh.put(temp.get());
                }
                page++;
                System.out.println("next page");
                temp=pageFileChannel.readPage(page);
                for(int l=0;l<4-remain;l++){
                    hh.put(temp.get());
                }
                hh.clear();
                length=hh.getInt();
            }
            else length=temp.getInt();
            System.out.println("length: "+length);//
 //////////////
            int offset;
            if(checknextpage(temp)){
                int remain=temp.remaining();
                ByteBuffer jj=ByteBuffer.allocate(4);
                for(int l=0;l<remain;l++){
                    jj.put(temp.get());
                }
                page++;
                System.out.println("next page");
                temp=pageFileChannel.readPage(page);
                for(int l=0;l<4-remain;l++){
                    jj.put(temp.get());
                }
                jj.clear();
                offset=jj.getInt();
            }
            else offset=temp.getInt();
            System.out.println("offset "+offset);//
////////////////////////
            List<Integer> keyword_list=new ArrayList<>();
            int page1=offset/PageFileChannel.PAGE_SIZE;
            int remain1=offset%PageFileChannel.PAGE_SIZE;
            ByteBuffer temp2=pageFileChannel.readPage(page1);
            temp2.position(remain1);
            for(int j=0;j<length;j++){
                int ss;
                if(checknextpage(temp2)){
                    int remain=temp2.remaining();
                    ByteBuffer jj=ByteBuffer.allocate(4);
                    for(int l=0;l<remain;l++){
                        jj.put(temp2.get());
                    }
                    page1++;
                    System.out.println("next page");
                    temp2=pageFileChannel.readPage(page1);
                    for(int l=0;l<4-remain;l++){
                        jj.put(temp2.get());
                    }
                    jj.clear();
                    ss=jj.getInt();
                }
                else ss=temp2.getInt();
                System.out.println("outnum: "+ss);
                keyword_list.add(ss);

            }
            plist.put(keyword,keyword_list);


        }
        pageFileChannel.close();
        InvertedIndexSegmentForTest invertedIndexSegmentForTest=new InvertedIndexSegmentForTest(plist,docs);
        return invertedIndexSegmentForTest;
    }


}
