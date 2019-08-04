package edu.uci.ics.cs221.index.inverted;

import com.google.common.base.Preconditions;
import edu.uci.ics.cs221.analysis.Analyzer;
import edu.uci.ics.cs221.analysis.ComposableAnalyzer;
import edu.uci.ics.cs221.analysis.PorterStemmer;
import edu.uci.ics.cs221.analysis.PunctuationTokenizer;
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

public class InvertedIndexManager_chenghao{
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
    private Map<Integer,String> db_pathmap=new HashMap<>();

    public static int DEFAULT_FLUSH_THRESHOLD = 1000;
    public static int DEFAULT_MERGE_THRESHOLD = 8;

    private InvertedIndexManager_chenghao(String indexFolder, Analyzer analyzer) {
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

    private String set_path_db(String i){
        String path;
        path=(indexFolder+new Timestamp(System.currentTimeMillis()).toString()
                .replace(" ", "_")
                .replace("-", "")
                .replace(":", "")
                .replace(".", "")
                + "_"
                + (int) (Math.random() * 1000 + 1) % 1000+".db");
        return path;
    }

    public static InvertedIndexManager_chenghao createOrOpen(String indexFolder, Analyzer analyzer) {
        try {
            Path indexFolderPath = Paths.get(indexFolder);
            if (Files.exists(indexFolderPath) && Files.isDirectory(indexFolderPath)) {
                if (Files.isDirectory(indexFolderPath)) {
                    return new InvertedIndexManager_chenghao(indexFolder, analyzer);
                } else {
                    throw new RuntimeException(indexFolderPath + " already exists and is not a directory");
                }
            } else {
                Files.createDirectories(indexFolderPath);
                return new InvertedIndexManager_chenghao(indexFolder, analyzer);
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
        if(documenttext==""&&documents!=null&&invertlist!=null) seg_counter++;
        if(documents.size()==0||documents==null||invertlist.size()==0||invertlist==null) return;
        dbpath=set_path_db(""+seg_counter);
        db_pathmap.put(seg_counter,dbpath);
        dbDocStore = MapdbDocStore.createWithBulkLoad(dbpath, documents.entrySet().iterator());
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
            // System.out.println("temp: "+temp);
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
        System.out.println("flush and segment ++");
        documents = new TreeMap<>();
        invertlist=new TreeMap<>();

    }

    public void mergeAllSegments() {
        // merge only happens at even number of segments
        //data(every key words length)
        //path(for page channel)
        //path db
        //db
        //disk
        Preconditions.checkArgument(getNumSegments() % 2 == 0);
        int n=getNumSegments();
        Map<Integer,List<Integer>> data_new=new HashMap<>();
        Map<Integer,Path> pathmap_new=new HashMap<>();
        Map<Integer,String> dbpath_new=new HashMap<>();
        for(int i=0;i<n;i=i+2) {
            DocumentStore db1=MapdbDocStore.createOrOpen(db_pathmap.get(i));
            DocumentStore db2=MapdbDocStore.createOrOpen(db_pathmap.get(i+1));
            int ndb1=(int)db1.size();
            int ndb2=(int)db2.size();
            Map<Integer, Document> documents_merge=new TreeMap<>();
            for(int j=0;j<ndb1;j++){
                Document temp_doc=db1.getDocument(j);
                documents_merge.put(j,temp_doc);

            }
            for(int j=0;j<ndb2;j++){
                Document temp_doc=db2.getDocument(j);
                documents_merge.put(j+ndb1,temp_doc);
            }
            db2.close();
            db1.close();
            File temp_file1 = new File(db_pathmap.get(i));
            temp_file1.delete();
            File temp_file2 = new File(db_pathmap.get(i+1));
            temp_file2.delete();
            String db_temppath=set_path_db(i+"");
            dbpath_new.put(i,db_temppath);
            DocumentStore db_new=MapdbDocStore.createWithBulkLoad(db_temppath,documents_merge.entrySet().iterator());
            db_new.close();


            PageFileChannel temp1= PageFileChannel.createOrOpen(pathmap.get(i));
            PageFileChannel temp2= PageFileChannel.createOrOpen(pathmap.get(i+1));
            int length1=data.get(i).size();
            int length2=data.get(i+1).size();
            Map<String,List<Integer>>merge1=new TreeMap<>();
            Map<String,List<Integer>>merge2=new TreeMap<>();
            Map<String,List<Integer>>merge_new=new TreeMap<>();
            /////////////////////////
            int page=0;
            ByteBuffer temp=temp1.readPage(0);
            temp.clear();
            for(int ii=0;ii<length1;ii++){
                char[] words=new char[data.get(i).get(ii)];
                for(int j=0;j<words.length;j++){
                    if(checknextpage(temp)){
                        page++;
                        temp=temp1.readPage(page);
                    }
                    byte uu=temp.get();
                    words[j]=(char)uu;
                }
                String keyword= String.valueOf(words);
                System.out.println(temp.position()+" word: "+keyword+" number: "+words.length);
                int reminder=20-words.length;
                for(int j=0;j<reminder;j++) {
                    if(temp.remaining()==0){
                        page++;
                        temp=temp1.readPage(page);
                    }
                    temp.get();
                }

                int length;
                List<Integer> merge1_int=new ArrayList<>();
                if(checknextpage(temp)){
                    int remain=temp.remaining();
                    ByteBuffer hh=ByteBuffer.allocate(4);
                    for(int l=0;l<remain;l++){
                        hh.put(temp.get());
                    }
                    page++;
                    System.out.println("next page");
                    temp=temp1.readPage(page);
                    for(int l=0;l<4-remain;l++){
                        hh.put(temp.get());
                    }
                    hh.clear();
                    length=hh.getInt();
                }
                else length=temp.getInt();
                System.out.println("length: "+length);//
                merge1_int.add(length);
                int offset;
                if(checknextpage(temp)){
                    int remain=temp.remaining();
                    ByteBuffer jj=ByteBuffer.allocate(4);
                    for(int l=0;l<remain;l++){
                        jj.put(temp.get());
                    }
                    page++;
                    System.out.println("next page");
                    temp=temp1.readPage(page);
                    for(int l=0;l<4-remain;l++){
                        jj.put(temp.get());
                    }
                    jj.clear();
                    offset=jj.getInt();
                }
                else offset=temp.getInt();
                merge1_int.add(offset);
                merge1.put(keyword,merge1_int);

            }
            ////////////////////
            int page_second=0;
            ByteBuffer temp_second=temp2.readPage(0);
            temp_second.clear();
            for(int ii=0;ii<length2;ii++){
                char[] words=new char[data.get(i+1).get(ii)];
                for(int j=0;j<words.length;j++){
                    if(checknextpage(temp_second)){
                        page_second++;
                        temp_second=temp2.readPage(page_second);
                    }
                    byte uu=temp_second.get();
                    words[j]=(char)uu;
                }
                String keyword= String.valueOf(words);
                //System.out.println(temp.position()+" word: "+keyword+" number: "+words.length);
                int reminder=20-words.length;
                for(int j=0;j<reminder;j++) {
                    if(temp_second.remaining()==0){
                        page_second++;
                        temp_second=temp2.readPage(page_second);
                    }
                    temp_second.get();
                }

                int length;
                List<Integer> merge2_int=new ArrayList<>();
                if(checknextpage(temp_second)){
                    int remain=temp_second.remaining();
                    ByteBuffer hh=ByteBuffer.allocate(4);
                    for(int l=0;l<remain;l++){
                        hh.put(temp_second.get());
                    }
                    page_second++;
                    System.out.println("next page");
                    temp_second=temp2.readPage(page_second);
                    for(int l=0;l<4-remain;l++){
                        hh.put(temp_second.get());
                    }
                    hh.clear();
                    length=hh.getInt();
                }
                else length=temp_second.getInt();
                System.out.println("length: "+length);//
                merge2_int.add(length);
                int offset;
                if(checknextpage(temp_second)){
                    int remain=temp_second.remaining();
                    ByteBuffer jj=ByteBuffer.allocate(4);
                    for(int l=0;l<remain;l++){
                        jj.put(temp_second.get());
                    }
                    page_second++;
                    temp_second=temp2.readPage(page_second);
                    for(int l=0;l<4-remain;l++){
                        jj.put(temp_second.get());
                    }
                    jj.clear();
                    offset=jj.getInt();
                }
                else offset=temp_second.getInt();
                merge2_int.add(offset);
                merge2.put(keyword,merge2_int);
            }

            Iterator iter1=merge1.entrySet().iterator();
            Iterator iter2=merge2.entrySet().iterator();





            merge1.clear();
            merge2.clear();
            merge_new.clear();
            temp2.close();
            temp1.close();

        }
        db_pathmap.clear();
        data.clear();
        pathmap.clear();
        for(int i=0;i<dbpath_new.size();i++){
            db_pathmap.put(i,dbpath_new.get(i));
        }

        for(int i=0;i<data_new.size();i++){
            data.put(i,data_new.get(i));
        }

        for(int i=0;i<pathmap_new.size();i++){
            pathmap.put(i,pathmap_new.get(i));
        }
        seg_counter=seg_counter/2;
    }

    public Iterator<Document> searchQuery(String keyword1) {
        Preconditions.checkNotNull(keyword1);
        PorterStemmer stemmer=new PorterStemmer();
        String temp_new=stemmer.stem(keyword1);
        List<Document> list1=new ArrayList<>();
        int nn=getNumSegments();
        for(int ii=0;ii<nn;ii++){
            Path aa=pathmap.get(ii);
            pageFileChannel=PageFileChannel.createOrOpen(aa);
            dbpath=db_pathmap.get(ii);
            dbDocStore = MapdbDocStore.createOrOpen(dbpath);
            int n=data.get(ii).size();//size
            List<Integer> list=data.get(ii);
            int page=0;
            ByteBuffer temp=pageFileChannel.readPage(0);
            temp.clear();
            for(int i=0;i<n;i++){
                char[] words=new char[list.get(i)];
                for(int j=0;j<list.get(i);j++){
                    if(checknextpage(temp)){
                        page++;
                        temp=pageFileChannel.readPage(page);
                    }
                    byte uu=temp.get();
                    words[j]=(char)uu;
                }

                String keyword= String.valueOf(words);
                int reminder=20-list.get(i);
                for(int j=0;j<reminder;j++) {
                    if(temp.remaining()==0){
                        page++;
                        temp=pageFileChannel.readPage(page);
                    }
                    temp.get();
                }


                int length;
                if(checknextpage(temp)){
                    int remain=temp.remaining();
                    ByteBuffer hh=ByteBuffer.allocate(4);
                    for(int l=0;l<remain;l++)
                        hh.put(temp.get());
                    page++;
                    temp=pageFileChannel.readPage(page);
                    for(int l=0;l<4-remain;l++){
                        hh.put(temp.get());
                    }
                    hh.clear();
                    length=hh.getInt();
                }
                else length=temp.getInt();

                //////////////
                int offset;
                if(checknextpage(temp)){
                    int remain=temp.remaining();
                    ByteBuffer jj=ByteBuffer.allocate(4);
                    for(int l=0;l<remain;l++){
                        jj.put(temp.get());
                    }
                    page++;
                    temp=pageFileChannel.readPage(page);
                    for(int l=0;l<4-remain;l++){
                        jj.put(temp.get());
                    }
                    jj.clear();
                    offset=jj.getInt();
                }
                else offset=temp.getInt();


                if(keyword.equals(temp_new)){
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
                            temp2=pageFileChannel.readPage(page1);
                            for(int l=0;l<4-remain;l++){
                                jj.put(temp2.get());
                            }
                            jj.clear();
                            ss=jj.getInt();
                        }
                        else ss=temp2.getInt();

                        Document dd=dbDocStore.getDocument(ss);
                        list1.add(dd);
                    }
                }
            }
            pageFileChannel.close();
            dbDocStore.close();
        }
        Iterator<Document> iterator=list1.iterator();
        return iterator;
    }

    public Iterator<Document> searchAndQuery(List<String> keywords) {
        Preconditions.checkNotNull(keywords);
        List<String> newkey=new ArrayList<>();
        PorterStemmer stemmer=new PorterStemmer();
        for(int i=0;i<keywords.size();i++)
            newkey.add(stemmer.stem(keywords.get(i)));
        List<Document> list1=new ArrayList<>();
        int nn=getNumSegments();
        for(int ii=0;ii<nn;ii++){
            boolean flag_read=true;
            Path aa=pathmap.get(ii);
            System.out.println("segemnt: "+ii);
            pageFileChannel=PageFileChannel.createOrOpen(aa);
            dbpath=db_pathmap.get(ii);
            dbDocStore = MapdbDocStore.createOrOpen(dbpath);
            List<Integer> document_Id=new ArrayList<>();
            int n=data.get(ii).size();//size
            List<Integer> list=data.get(ii);
            int page=0;
            ByteBuffer temp=pageFileChannel.readPage(0);
            temp.clear();
            for(int i=0;i<n;i++){
                char[] words=new char[list.get(i)];
                for(int j=0;j<list.get(i);j++){
                    if(checknextpage(temp)){
                        page++;
                        temp=pageFileChannel.readPage(page);
                    }
                    byte uu=temp.get();
                    words[j]=(char)uu;
                }

                String keyword= String.valueOf(words);
                int reminder=20-list.get(i);
                for(int j=0;j<reminder;j++) {
                    if(temp.remaining()==0){
                        page++;
                        temp=pageFileChannel.readPage(page);
                    }
                    temp.get();
                }

                int length;   //first length
                if(checknextpage(temp)){
                    int remain=temp.remaining();
                    ByteBuffer hh=ByteBuffer.allocate(4);
                    for(int l=0;l<remain;l++)
                        hh.put(temp.get());
                    page++;
                    temp=pageFileChannel.readPage(page);
                    for(int l=0;l<4-remain;l++){
                        hh.put(temp.get());
                    }
                    hh.clear();
                    length=hh.getInt();
                }
                else length=temp.getInt();

                int offset;   //then offset
                if(checknextpage(temp)){
                    int remain=temp.remaining();
                    ByteBuffer jj=ByteBuffer.allocate(4);
                    for(int l=0;l<remain;l++){
                        jj.put(temp.get());
                    }
                    page++;
                    temp=pageFileChannel.readPage(page);
                    for(int l=0;l<4-remain;l++){
                        jj.put(temp.get());
                    }
                    jj.clear();
                    offset=jj.getInt();
                }
                else offset=temp.getInt();


                boolean flag=false;
                for(int u=0;u<newkey.size();u++){
                    if(newkey.get(u).equals(keyword)) {
                        flag = true;
                        break;
                    }
                }
                if(flag){
                    int page1=offset/PageFileChannel.PAGE_SIZE;
                    int remain1=offset%PageFileChannel.PAGE_SIZE;
                    ByteBuffer temp2=pageFileChannel.readPage(page1);
                    temp2.position(remain1);
                    List<Integer> check=new ArrayList<>();
                    for(int j=0;j<length;j++){
                        int ss;
                        if(checknextpage(temp2)){
                            int remain=temp2.remaining();
                            ByteBuffer jj=ByteBuffer.allocate(4);
                            for(int l=0;l<remain;l++){
                                jj.put(temp2.get());
                            }
                            page1++;
                            temp2=pageFileChannel.readPage(page1);
                            for(int l=0;l<4-remain;l++){
                                jj.put(temp2.get());
                            }
                            jj.clear();
                            ss=jj.getInt();
                        }
                        else ss=temp2.getInt();

                        if(flag_read) {

                            document_Id.add(ss);
                            System.out.println(keyword+"di yi ci llllll");
                        }
                        else{
                            check.add(ss);
                        }
                    }
                    flag_read=false;
                    if(check.size()!=0&&document_Id.size()!=0){
                        //do the intersection
                        List<Integer> temp_merge=new ArrayList();
                        for(int i1=0;i1<document_Id.size();i1++) {
                            for(int i2=0;i2<check.size();i2++) {
                                if(check.get(i2)==(document_Id.get(i1))) {
                                    temp_merge.add(check.get(i2));
                                    System.out.println("add: "+check.get(i2));
                                }
                            }
                        }
                        document_Id.clear();
                        //System.out.println("size: "+document_Id.size());
                        if(temp_merge.size()!=0) {
                            for (int jj = 0; jj < temp_merge.size(); jj++) {

                                document_Id.add(temp_merge.get(jj));
                                System.out.print(temp_merge.get(jj));
                            }
                        }
                        System.out.println();
                        temp_merge.clear();

                    }
                    check.clear();
                }
            }
            if(document_Id.size()!=0) {
                for (int ll = 0; ll < document_Id.size(); ll++) {
                    Document dd = dbDocStore.getDocument(document_Id.get(ll));
                    list1.add(dd);
                }

            }
            document_Id.clear();
            pageFileChannel.close();
            dbDocStore.close();
        }
        Iterator<Document> iter=list1.iterator();
        return iter;
    }

    public Iterator<Document> searchOrQuery(List<String> keywords) {
        Preconditions.checkNotNull(keywords);
        List<String> newkey=new ArrayList<>();
        PorterStemmer stemmer=new PorterStemmer();
        for(int i=0;i<keywords.size();i++)
            newkey.add(stemmer.stem(keywords.get(i)));
        List<Document> list1=new ArrayList<>();
        int nn=getNumSegments();
        for(int ii=0;ii<nn;ii++){
            Path aa=pathmap.get(ii);
            List<Integer> document_Id=new ArrayList<>();
            pageFileChannel=PageFileChannel.createOrOpen(aa);
            dbpath=db_pathmap.get(ii);
            dbDocStore = MapdbDocStore.createOrOpen(dbpath);
            int n=data.get(ii).size();//size
            List<Integer> list=data.get(ii);
            int page=0;
            ByteBuffer temp=pageFileChannel.readPage(0);
            temp.clear();
            for(int i=0;i<n;i++){
                char[] words=new char[list.get(i)];
                for(int j=0;j<list.get(i);j++){
                    if(checknextpage(temp)){
                        page++;
                        temp=pageFileChannel.readPage(page);
                    }
                    byte uu=temp.get();
                    words[j]=(char)uu;
                }

                String keyword= String.valueOf(words);
                int reminder=20-list.get(i);
                for(int j=0;j<reminder;j++) {
                    if(temp.remaining()==0){
                        page++;
                        temp=pageFileChannel.readPage(page);
                    }
                    temp.get();
                }
                int length;
                if(checknextpage(temp)){
                    int remain=temp.remaining();
                    ByteBuffer hh=ByteBuffer.allocate(4);
                    for(int l=0;l<remain;l++)
                        hh.put(temp.get());
                    page++;
                    temp=pageFileChannel.readPage(page);
                    for(int l=0;l<4-remain;l++){
                        hh.put(temp.get());
                    }
                    hh.clear();
                    length=hh.getInt();
                }
                else length=temp.getInt();

                //////////////
                int offset;
                if(checknextpage(temp)){
                    int remain=temp.remaining();
                    ByteBuffer jj=ByteBuffer.allocate(4);
                    for(int l=0;l<remain;l++){
                        jj.put(temp.get());
                    }
                    page++;
                    temp=pageFileChannel.readPage(page);
                    for(int l=0;l<4-remain;l++){
                        jj.put(temp.get());
                    }
                    jj.clear();
                    offset=jj.getInt();
                }
                else offset=temp.getInt();
                boolean flag=false;
                for(int u=0;u<newkey.size();u++){
                    if(newkey.get(u).equals(keyword)) {
                        flag = true;
                        break;
                    }
                }

                if(flag){
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
                            temp2=pageFileChannel.readPage(page1);
                            for(int l=0;l<4-remain;l++){
                                jj.put(temp2.get());
                            }
                            jj.clear();
                            ss=jj.getInt();
                        }
                        else ss=temp2.getInt();
                        if(document_Id.size()==0) {
                            document_Id.add(ss);
                            Document dd = dbDocStore.getDocument(ss);
                            list1.add(dd);
                        }
                        else{
                            boolean ff=true;
                            for(int y=0;y<document_Id.size();y++){
                                if(document_Id.get(y).intValue()==ss){
                                    ff=false;
                                    break;
                                }
                            }
                            if(ff) {
                                document_Id.add(ss);
                                Document dd = dbDocStore.getDocument(ss);
                                list1.add(dd);
                            }
                        }
                    }
                }
            }
            pageFileChannel.close();
            dbDocStore.close();
        }
        Iterator<Document> iter=list1.iterator();
        return iter;
    }

    public Iterator<Document> documentIterator() {
        List<Document> list=new ArrayList<>();
        int n=getNumSegments();
        for(int i=0;i<n;i++){
            dbpath=db_pathmap.get(i);
            dbDocStore = MapdbDocStore.createOrOpen(dbpath);
            int total=(int)dbDocStore.size();
            for(int j=0;j<total;j++){
                Document dd=dbDocStore.getDocument(j);
                list.add(dd);
            }
            dbDocStore.close();
        }

        Iterator<Document> iterator=list.iterator();
        return iterator;

        // throw new UnsupportedOperationException();
    }

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
        dbpath=db_pathmap.get(segmentNum);
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
