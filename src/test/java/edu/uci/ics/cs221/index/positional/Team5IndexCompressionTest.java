package edu.uci.ics.cs221.index.positional;

import com.google.common.primitives.Ints;
import edu.uci.ics.cs221.analysis.Analyzer;
import edu.uci.ics.cs221.analysis.ComposableAnalyzer;
import edu.uci.ics.cs221.analysis.PorterStemmer;
import edu.uci.ics.cs221.analysis.PunctuationTokenizer;
import edu.uci.ics.cs221.index.inverted.DeltaVarLenCompressor;
import edu.uci.ics.cs221.index.inverted.InvertedIndexManager;
import edu.uci.ics.cs221.index.inverted.NaiveCompressor;
import edu.uci.ics.cs221.storage.Document;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class Team5IndexCompressionTest {
  private DeltaVarLenCompressor compressor = new DeltaVarLenCompressor();
  private NaiveCompressor naiveCompressor = new NaiveCompressor();
  private List<Integer> positional_list=new ArrayList<>();

  @Before
  public void setup() {
    for(int i=0;i<100;i++){
      positional_list.add(i);
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
    positional_list.clear();

  }
}
