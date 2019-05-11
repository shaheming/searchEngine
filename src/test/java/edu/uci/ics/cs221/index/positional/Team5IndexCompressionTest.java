package edu.uci.ics.cs221.index.positional;

import com.google.common.primitives.Ints;
import edu.uci.ics.cs221.analysis.Analyzer;
import edu.uci.ics.cs221.analysis.ComposableAnalyzer;
import edu.uci.ics.cs221.analysis.PorterStemmer;
import edu.uci.ics.cs221.analysis.PunctuationTokenizer;
import edu.uci.ics.cs221.index.inverted.DeltaVarLenCompressor;
import edu.uci.ics.cs221.index.inverted.InvertedIndexManager;
import edu.uci.ics.cs221.index.inverted.NaiveCompressor;

import java.io.File;
import java.util.*;

import edu.uci.ics.cs221.index.inverted.PageFileChannel;
import edu.uci.ics.cs221.storage.Document;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.print.Doc;
import java.lang.reflect.Array;

import static org.junit.Assert.assertFalse;

public class Team5IndexCompressionTest {
  private DeltaVarLenCompressor compressor = new DeltaVarLenCompressor();
  private NaiveCompressor naivecompressor = new NaiveCompressor();
  private String path1 = "./index/Team5IndexCompressionTest/naive_compress";
  private String path2 = "./index/Team5IndexCompressionTest/compress";
  private Analyzer analyzer = new ComposableAnalyzer(new PunctuationTokenizer(), new PorterStemmer());
  private InvertedIndexManager positional_list_naive_compressor;
  private InvertedIndexManager positional_list_compressor;


  @Before
  public void setup() {
    File directory1 = new File(path1);
    if (!directory1.exists()) {
      directory1.mkdirs();
    }
    File directory2 = new File(path2);
    if (!directory2.exists()) {
      directory2.mkdirs();
    }
    positional_list_naive_compressor=InvertedIndexManager.createOrOpenPositional(path1,analyzer,naivecompressor);
    positional_list_compressor=InvertedIndexManager.createOrOpenPositional(path2,analyzer,compressor);
  }

  //test empty document input
  @Test
  public void Test1() {
    Assert.assertEquals(0, PageFileChannel.readCounter);
    Assert.assertEquals(0, PageFileChannel.writeCounter);
    for(int i=0;i<10000;i++)
      positional_list_naive_compressor.addDocument(new Document(""));
    positional_list_naive_compressor.flush();
    for(int i=0;i<positional_list_naive_compressor.getNumSegments();i++){
      positional_list_naive_compressor.getIndexSegmentPositional(i);
    }
    int naive_wc=PageFileChannel.writeCounter;
    int naive_rc=PageFileChannel.readCounter;
    PageFileChannel.resetCounters();

    for(int i=0;i<10000;i++)
      positional_list_compressor.addDocument(new Document(""));
    positional_list_compressor.flush();
    for(int i=0;i<positional_list_compressor.getNumSegments();i++){
      positional_list_compressor.getIndexSegmentPositional(i);
    }
    int compress_wc=PageFileChannel.writeCounter;
    int compress_rc=PageFileChannel.readCounter;

    Assert.assertTrue(naive_rc==compress_rc);//0
    Assert.assertTrue(naive_wc==compress_wc);//0
  }

  //test simple documents with same text, each key word show only one time each document
  //mainly test inverted list since inverted list is long but positional list is short
  @Test
  public void Test2() {
    Assert.assertEquals(0, PageFileChannel.readCounter);
    Assert.assertEquals(0, PageFileChannel.writeCounter);
    for(int i=0;i<10000;i++)
      positional_list_naive_compressor.addDocument(new Document("cat Dot"));
    positional_list_naive_compressor.flush();
    for(int i=0;i<positional_list_naive_compressor.getNumSegments();i++){
      positional_list_naive_compressor.getIndexSegmentPositional(i);
    }
    int naive_wc=PageFileChannel.writeCounter;
    int naive_rc=PageFileChannel.readCounter;
    PageFileChannel.resetCounters();

    for(int i=0;i<10000;i++)
      positional_list_compressor.addDocument(new Document("cat Dot"));
    positional_list_compressor.flush();
    for(int i=0;i<positional_list_compressor.getNumSegments();i++){
      positional_list_compressor.getIndexSegmentPositional(i);
    }
    int compress_wc=PageFileChannel.writeCounter;
    int compress_rc=PageFileChannel.readCounter;

    Assert.assertTrue(naive_rc>1.5*compress_rc);
    Assert.assertTrue(naive_wc>1.5*compress_wc);
  }

  //test docs with different text and each key word show multiple times in multiple document
  //mainly test inverted list since inverted list is long but positional list is short
  @Test
  public void Test3() {
    Assert.assertEquals(0, PageFileChannel.readCounter);
    Assert.assertEquals(0, PageFileChannel.writeCounter);
    for(int i=0;i<3000;i++){
      positional_list_naive_compressor.addDocument(new Document("cat Dot cat Dog I can not tell the difference between cat and Dog"));
      positional_list_naive_compressor.addDocument(new Document("cat and dog have a lot of difference"));
      positional_list_naive_compressor.addDocument(new Document("Dog can be very different from cat"));
    }
    positional_list_naive_compressor.flush();
    for(int i=0;i<positional_list_naive_compressor.getNumSegments();i++){
      positional_list_naive_compressor.getIndexSegmentPositional(i);
    }
    int naive_wc=PageFileChannel.writeCounter;
    int naive_rc=PageFileChannel.readCounter;
    PageFileChannel.resetCounters();

    for(int i=0;i<3000;i++){
      positional_list_compressor.addDocument(new Document("cat Dot cat Dog I can not tell the difference between cat and Dog"));
      positional_list_compressor.addDocument(new Document("cat and dog have a lot of difference"));
      positional_list_compressor.addDocument(new Document("Dog can be very different from cat"));
    }
    positional_list_compressor.flush();
    for(int i=0;i<positional_list_compressor.getNumSegments();i++){
      positional_list_compressor.getIndexSegmentPositional(i);
    }
    int compress_wc=PageFileChannel.writeCounter;
    int compress_rc=PageFileChannel.readCounter;

    Assert.assertTrue(naive_rc>1.5*compress_rc);
    Assert.assertTrue(naive_wc>1.5*compress_wc);
  }

  //test docs with different text and each key word show multiple times only in a document
  //mainly test positional  list since inverted  since  positional list is long
  @Test
  public void Test4() {
    Assert.assertEquals(0, PageFileChannel.readCounter);
    Assert.assertEquals(0, PageFileChannel.writeCounter);
    for(int i=0;i<3000;i++){
      positional_list_naive_compressor.addDocument(new Document
              ("cat"+i+" cat"+i+" cat"+i+" and dog"+i+" dog"+i+" dog"+i));
      positional_list_naive_compressor.addDocument(new Document
              ("pepsi"+i+" pepsi"+i+" pepsi"+i+" or coke"+i+" coke"+i+" coke"+i));
      positional_list_naive_compressor.addDocument(new Document
              ("microsoft"+i+" microsoft"+i+" microsoft"+i+" vs apple"+i+" apple"+i+" apple"+i));
    }
    positional_list_naive_compressor.flush();
    for(int i=0;i<positional_list_naive_compressor.getNumSegments();i++){
      positional_list_naive_compressor.getIndexSegmentPositional(i);
    }
    int naive_wc=PageFileChannel.writeCounter;
    int naive_rc=PageFileChannel.readCounter;
    PageFileChannel.resetCounters();

    for(int i=0;i<3000;i++){
      positional_list_compressor.addDocument(new Document
              ("cat"+i+" cat"+i+" cat"+i+" and dog"+i+" dog"+i+" dog"+i));
      positional_list_compressor.addDocument(new Document
              ("pepsi"+i+" pepsi"+i+" pepsi"+i+" or coke"+i+" coke"+i+" coke"+i));
      positional_list_compressor.addDocument(new Document
              ("microsoft"+i+" microsoft"+i+" microsoft"+i+" vs apple"+i+" apple"+i+" apple"+i));
    }
    positional_list_compressor.flush();
    for(int i=0;i<positional_list_compressor.getNumSegments();i++){
      positional_list_compressor.getIndexSegmentPositional(i);
    }
    int compress_wc=PageFileChannel.writeCounter;
    int compress_rc=PageFileChannel.readCounter;

    Assert.assertTrue(naive_rc>1.5*compress_rc);
    Assert.assertTrue(naive_wc>1.5*compress_wc);
  }

  //test add really long document, testing the positional list works or not
  //mainly test positional  list since inverted  since  positional list is long

  public void Test5() {
    Assert.assertEquals(0, PageFileChannel.readCounter);
    Assert.assertEquals(0, PageFileChannel.writeCounter);
    String doc1="cat Dot cat Dog I can not tell the difference between cat and Dog ";
    String doc2="cat and dog have a lot of difference ";
    String doc3="Dog can be very different from cat ";
    for(int i=0;i<1000;i++){
      doc1=doc1+"cat Dot cat Dog I can not tell the difference between cat and Dog ";
      doc2=doc2+"cat and dog have a lot of difference ";
      doc3=doc3+"Dog can be very different from cat ";
    }

    Document document1=new Document(doc1);
    Document document2=new Document(doc2);
    Document document3=new Document(doc3);


    for(int i=0;i<1000;i++){
      positional_list_naive_compressor.addDocument(document1);
      positional_list_naive_compressor.addDocument(document2);
      positional_list_naive_compressor.addDocument(document3);
    }
    positional_list_naive_compressor.flush();
    for(int i=0;i<positional_list_naive_compressor.getNumSegments();i++){
      positional_list_naive_compressor.getIndexSegmentPositional(i);
    }
    int naive_wc=PageFileChannel.writeCounter;
    int naive_rc=PageFileChannel.readCounter;
    PageFileChannel.resetCounters();

    for(int i=0;i<30;i++){
      positional_list_compressor.addDocument(document1);
      positional_list_compressor.addDocument(document2);
      positional_list_compressor.addDocument(document3);
    }
    positional_list_compressor.flush();
    for(int i=0;i<positional_list_compressor.getNumSegments();i++){
      positional_list_compressor.getIndexSegmentPositional(i);
    }
    int compress_wc=PageFileChannel.writeCounter;
    int compress_rc=PageFileChannel.readCounter;

    Assert.assertTrue(naive_rc>1.5*compress_rc);
    Assert.assertTrue(naive_wc>1.5*compress_wc);
  }



  @After
  public void cleanup()  {
    PageFileChannel.resetCounters();
    File f = new File("./index/Team5IndexCompressionTest");
    File[] files = f.listFiles();
    for (File file : files) {
      file.delete();
    }
    f.delete();
  }
}
