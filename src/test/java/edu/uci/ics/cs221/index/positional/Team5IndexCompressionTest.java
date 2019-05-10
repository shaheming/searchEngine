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

import java.lang.reflect.Array;

import static org.junit.Assert.assertFalse;

public class Team5IndexCompressionTest {
  private DeltaVarLenCompressor compressor = new DeltaVarLenCompressor();
  private NaiveCompressor naiveCompressor = new NaiveCompressor();
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
    positional_list_naive_compressor=InvertedIndexManager.createOrOpenPositional(path1,analyzer,compressor);
    positional_list_compressor=InvertedIndexManager.createOrOpenPositional(path2,analyzer,naiveCompressor);
  }

  //test empty document input
  @Test
  public void Test1() {
    Assert.assertEquals(0, PageFileChannel.readCounter);
    Assert.assertEquals(0, PageFileChannel.writeCounter);
    for(int i=0;i<10000;i++)
      positional_list_naive_compressor.addDocument(new Document(""));
    for(int i=0;i<positional_list_naive_compressor.getNumSegments();i++){
      positional_list_naive_compressor.getIndexSegmentPositional(i);
    }
    int naive_wc=PageFileChannel.writeCounter;
    int naive_rc=PageFileChannel.readCounter;
    PageFileChannel.resetCounters();

    for(int i=0;i<10000;i++)
      positional_list_compressor.addDocument(new Document(""));
    for(int i=0;i<positional_list_compressor.getNumSegments();i++){
      positional_list_compressor.getIndexSegmentPositional(i);
    }
    int compress_wc=PageFileChannel.writeCounter;
    int compress_rc=PageFileChannel.readCounter;

    Assert.assertTrue(naive_rc==compress_rc);//0
    Assert.assertTrue(naive_wc==compress_wc);//0
  }

  //test simple documents with same text, each key word show only one time each document
  @Test
  public void Test2() {
    Assert.assertEquals(0, PageFileChannel.readCounter);
    Assert.assertEquals(0, PageFileChannel.writeCounter);
    for(int i=0;i<10000;i++)
      positional_list_naive_compressor.addDocument(new Document("cat Dot"));
    for(int i=0;i<positional_list_naive_compressor.getNumSegments();i++){
      positional_list_naive_compressor.getIndexSegmentPositional(i);
    }
    int naive_wc=PageFileChannel.writeCounter;
    int naive_rc=PageFileChannel.readCounter;
    PageFileChannel.resetCounters();

    for(int i=0;i<10000;i++)
      positional_list_compressor.addDocument(new Document("cat Dot"));
    for(int i=0;i<positional_list_compressor.getNumSegments();i++){
      positional_list_compressor.getIndexSegmentPositional(i);
    }
    int compress_wc=PageFileChannel.writeCounter;
    int compress_rc=PageFileChannel.readCounter;

    Assert.assertTrue(naive_rc<compress_rc);
    Assert.assertTrue(naive_wc<compress_wc);
  }

  //test docs with different text and each key word show multiple times in multiple document
  @Test
  public void Test3() {
    Assert.assertEquals(0, PageFileChannel.readCounter);
    Assert.assertEquals(0, PageFileChannel.writeCounter);
    for(int i=0;i<3000;i++){
      positional_list_naive_compressor.addDocument(new Document("cat Dot cat Dog I can not tell the difference between cat and Dog"));
      positional_list_naive_compressor.addDocument(new Document("cat and dog have a lot of difference"));
      positional_list_naive_compressor.addDocument(new Document("Dog can be very different from cat"));
    }

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

    for(int i=0;i<positional_list_compressor.getNumSegments();i++){
      positional_list_compressor.getIndexSegmentPositional(i);
    }
    int compress_wc=PageFileChannel.writeCounter;
    int compress_rc=PageFileChannel.readCounter;

    Assert.assertTrue(naive_rc<compress_rc);
    Assert.assertTrue(naive_wc<compress_wc);
  }

  //test docs with different text and each key word show multiple times only in a document
  @Test
  public void Test4() {
    Assert.assertEquals(0, PageFileChannel.readCounter);
    Assert.assertEquals(0, PageFileChannel.writeCounter);
    for(int i=0;i<3000;i++){
      positional_list_naive_compressor.addDocument(new Document("cat cat cat and dog dog dog"));
      positional_list_naive_compressor.addDocument(new Document("pepsi pepsi pepsi or coke coke coke"));
      positional_list_naive_compressor.addDocument(new Document("microsoft microsoft microsoft vs apple apple apple"));
    }

    for(int i=0;i<positional_list_naive_compressor.getNumSegments();i++){
      positional_list_naive_compressor.getIndexSegmentPositional(i);
    }
    int naive_wc=PageFileChannel.writeCounter;
    int naive_rc=PageFileChannel.readCounter;
    PageFileChannel.resetCounters();

    for(int i=0;i<3000;i++){
      positional_list_compressor.addDocument(new Document("cat cat cat and dog dog dog"));
      positional_list_compressor.addDocument(new Document("pepsi pepsi pepsi or coke coke coke"));
      positional_list_compressor.addDocument(new Document("microsoft microsoft microsoft vs apple apple apple"));
    }

    for(int i=0;i<positional_list_compressor.getNumSegments();i++){
      positional_list_compressor.getIndexSegmentPositional(i);
    }
    int compress_wc=PageFileChannel.writeCounter;
    int compress_rc=PageFileChannel.readCounter;

    Assert.assertTrue(naive_rc<compress_rc);
    Assert.assertTrue(naive_wc<compress_wc);
  }



  /**
   * Test time consumption for compress a large amount of integer.
   * Test the correctness of encode and decode
   * @throws Exception
   */
  @Test
  public void stressTest() throws Exception {
    Random r = new Random(System.currentTimeMillis());
    int SIZE = 10000000;
    int data[] = r.ints(SIZE, 0, SIZE).toArray();
    System.out.println("Test compress " + SIZE + " ints.");
    Arrays.sort(data);
    List<Integer> list = Ints.asList(data);
    System.out.println("Start encode");
    long start = System.currentTimeMillis();
    DeltaVarLenCompressor compressor = new DeltaVarLenCompressor();
    byte[] encoded = compressor.encode(list);
    long finish = System.currentTimeMillis();
    long timeElapsed = finish - start;
    System.out.println("Encode end, use: " + timeElapsed * 1.0 / 1000 + " s");

    start = System.currentTimeMillis();
    System.out.println("Start decode");
    List<Integer> decoded = compressor.decode(encoded, 0, encoded.length);
    finish = System.currentTimeMillis();
    timeElapsed = finish - start;
    System.out.println("Decode end, use: " + timeElapsed * 1.0 / 1000 + " s");

    for (int i = 0; i < data.length; i++) {
      Assert.assertTrue(data[i] == decoded.get(i));
    }
  }

  /**
   * Test time consumption for compress a large amount of integer.
   * Test the correctness of encode and decode
   * @throws Exception
   */
  @Test
  public void stressTest2() throws Exception {
    Random r = new Random(System.currentTimeMillis());
    int SIZE = 100000000;
    int data[] = r.ints(SIZE, 0, SIZE).toArray();
    System.out.println("Test compress " + SIZE + " ints.");
    Arrays.sort(data);
    List<Integer> list = Ints.asList(data);
    System.out.println("Start encode");
    long start = System.currentTimeMillis();
    DeltaVarLenCompressor compressor = new DeltaVarLenCompressor();
    byte[] encoded = compressor.encode(list);
    long finish = System.currentTimeMillis();
    long timeElapsed = finish - start;
    System.out.println("Encode end, use: " + timeElapsed * 1.0 / 1000 + " s");

    start = System.currentTimeMillis();
    System.out.println("Start decode");
    List<Integer> decoded = compressor.decode(encoded, 0, encoded.length);
    finish = System.currentTimeMillis();
    timeElapsed = finish - start;
    System.out.println("Decode end, use: " + timeElapsed * 1.0 / 1000 + " s");

    for (int i = 0; i < data.length; i++) {
      Assert.assertTrue(data[i] == decoded.get(i));
    }
  }
  /* Test corner case
   *
   */
  @Test
  public void emptyInputTest() throws Exception {
    DeltaVarLenCompressor compressor = new DeltaVarLenCompressor();
    byte[] encoded = compressor.encode(new ArrayList<>());
    Assert.assertEquals(0, encoded.length);
    List<Integer> decoded = compressor.decode(encoded, 0, encoded.length);
    Assert.assertEquals(0, decoded.size());
  }

  /* Test corner case
   *
   */
  @Test
  public void commpressionRatioTest1() throws Exception {

    int SIZE = 10000000;
    Random r = new Random(System.currentTimeMillis());
    int data[] = r.ints(SIZE, 0, SIZE).toArray();
    System.out.println("Test compress " + SIZE + " ints.");
    Arrays.sort(data);
    List<Integer> list = Ints.asList(data);
    byte[] encoded = compressor.encode(list);
    byte[] naiveencoded = naiveCompressor.encode(list);
    System.out.println((double)encoded.length / naiveencoded.length);
    System.out.println(encoded.length + " "+ naiveencoded.length);
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
