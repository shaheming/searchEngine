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
import java.util.Collections;

import edu.uci.ics.cs221.index.inverted.PageFileChannel;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class Team5IndexCompressionTest {
  private DeltaVarLenCompressor compressor = new DeltaVarLenCompressor();
  private NaiveCompressor naiveCompressor = new NaiveCompressor();
  private String path1 = "./index/Team5IndexCompressionTest/naive_compress";
  private String path2 = "./index/Team5IndexCompressionTest/compress";
  private Analyzer analyzer = new ComposableAnalyzer(new PunctuationTokenizer(), new PorterStemmer());
  private InvertedIndexManager positional_list_naive_compressor;
  private InvertedIndexManager positional_list_compresspor;

  private List<Integer> positional_list_empty=new ArrayList<>();
  private List<Integer> positional_list1=new ArrayList<>();
  private List<Integer> positional_list2=new ArrayList<>();
  private List<Integer> positional_list3=new ArrayList<>();


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
    positional_list_compresspor=InvertedIndexManager.createOrOpenPositional(path2,analyzer,naiveCompressor);

    for(int i=0;i<100;i++) positional_list1.add(i);//small number(sorted)
    for(int i=0;i<10000;i++) positional_list2.add(i);//big number(sorted)
    for(int i=0;i<10000;i++) {
      positional_list3.add((int)Math.random()%100);//random number(sorted)
      Collections.sort(positional_list3);
    }

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

    positional_list_empty.clear();
    positional_list1.clear();
    positional_list2.clear();
    positional_list3.clear();
  }
}
