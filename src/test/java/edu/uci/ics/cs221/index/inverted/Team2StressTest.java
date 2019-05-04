package edu.uci.ics.cs221.index.inverted;

import edu.uci.ics.cs221.analysis.Analyzer;
import edu.uci.ics.cs221.analysis.ComposableAnalyzer;
import edu.uci.ics.cs221.analysis.PorterStemmer;
import edu.uci.ics.cs221.analysis.PunctuationTokenizer;
import edu.uci.ics.cs221.storage.Document;
import org.junit.After;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import static java.lang.System.exit;
import static org.junit.Assert.assertEquals;

public class Team2StressTest {

  static Analyzer analyzer;
  static InvertedIndexManager invertedIndexManager;

  /**
   * Instantiate the invertedIndexManager. Import and add external documents to get prepared for the
   * tests.
   *
   * <p>Finish this test within 20 min.
   */
  @Test(timeout = 1200000)
  public void initAndTest() {
    analyzer = new ComposableAnalyzer(new PunctuationTokenizer(), new PorterStemmer());
    invertedIndexManager = InvertedIndexManager.createOrOpen("./index/Team2StressTest/", analyzer);
    InvertedIndexManager.DEFAULT_FLUSH_THRESHOLD = 200;
    InvertedIndexManager.DEFAULT_MERGE_THRESHOLD = 6;
    String text = "";
    try {
      URL url =
          new URL(
              "https://grape.ics.uci.edu/wiki/public/raw-attachment/wiki/cs221-2019-spring-project2/Team2StressTest.txt");
      BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(url.openStream()));
      StringBuilder stringBuilder = new StringBuilder();
      String inputLine;
      while ((inputLine = bufferedReader.readLine()) != null) {
        stringBuilder.append(inputLine);
        stringBuilder.append(System.lineSeparator());
      }

      bufferedReader.close();
      text = stringBuilder.toString().trim();
    } catch (Exception e) {
      e.printStackTrace();
    }

    // copy full pride-and-prejudice for around 1500 times, every document is about 708KB, 1500
    // times is about 1GB
    for (int i = 0; i < 1500; i++) {
      invertedIndexManager.addDocument(new Document(text));
    }
    // Then, add two small test documents
    invertedIndexManager.addDocument(new Document("qwertyuiop elizabeth"));
    invertedIndexManager.addDocument(new Document("qwertyuiop"));
    invertedIndexManager.flush();

    try {
      test1();
    } catch (Throwable e) {
      System.out.println("Team2StressTest test1 FAILED");
      e.printStackTrace();
    }

    try {
      test2();
    } catch (Throwable e) {
      System.out.println("Team2StressTest test1 FAILED");
      e.printStackTrace();
    }
  }

  /**
   * Using search keywords query. Check whether the number of the documents containing "elizabeth"
   * is 5001, the purpose of doing this is to check whether the system can handle large amounts of
   * data and process it correctly
   */
  public void test1() {
    Iterator<Document> result = invertedIndexManager.searchQuery("elizabeth");

    int counter = 0;
    while (result.hasNext()) {
      counter++;
      result.next();
    }
    assertEquals(1501, counter);
  }

  /** Test whether searchAndQuery() works well in large datasets */
  public void test2() {
    List<String> keywords = Arrays.asList("qwertyuiop", "elizabeth");

    Iterator<Document> result = invertedIndexManager.searchAndQuery(keywords);

    int counter = 0;
    while (result.hasNext()) {
      counter++;
      result.next();
    }
    assertEquals(1, counter);
  }

  /**
   * Change back the flush threshold Delete all the files and empty or non-empty folders in
   * ./index/Team2StressTest/ Use recursive delete in case any sub folders is created
   */
  @After
  public void after() throws Exception {
    InvertedIndexManager.DEFAULT_FLUSH_THRESHOLD = 1000;
    InvertedIndexManager.DEFAULT_MERGE_THRESHOLD = 8;

    String path = "./index/Team2StressTest/";
    if (delAllFile(path)) {
      System.out.println("All files in " + path + " are deleted.");
    } else {
      System.out.println("Deletion of all files in " + path + " failed to complete.");
    }
  }

  static boolean delAllFile(String path) throws Exception {
    Path rootPath = Paths.get(path);
    Files.walk(rootPath)
        .sorted(Comparator.reverseOrder())
        .map(Path::toFile)
        .peek(System.out::println)
        .forEach(File::delete);
    Files.deleteIfExists(rootPath);
    return true;
  }
}
