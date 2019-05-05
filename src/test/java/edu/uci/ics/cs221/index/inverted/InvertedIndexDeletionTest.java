package edu.uci.ics.cs221.index.inverted;

import edu.uci.ics.cs221.analysis.Analyzer;
import edu.uci.ics.cs221.analysis.ComposableAnalyzer;
import edu.uci.ics.cs221.analysis.PorterStemmer;
import edu.uci.ics.cs221.analysis.PunctuationTokenizer;
import edu.uci.ics.cs221.storage.Document;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import static org.junit.Assert.*;

/**
 * For teams doing project 2 extra credits (deletion), please add all your own deletion test cases
 * in this class. The TA will only look at this class to give extra credit points.
 */
public class InvertedIndexDeletionTest {
  private String path = "./index/InvertedIndexDeletionTest";
  private Analyzer analyzer =
      new ComposableAnalyzer(new PunctuationTokenizer(), new PorterStemmer());
  private InvertedIndexManager invertedList;

  @Before
  public void setUp() throws Exception {
    // number = 10
    Document[] documents = {
      new Document("cat dog toy pig"),
      new Document("cat Dot"),
      new Document("cat dot toy"),
      new Document("cat toy Dog"),
      new Document("toy dog cat"),
      new Document("cat Dog bird")
    };
    File directory = new File(path);
    if (!directory.exists()) {
      directory.mkdirs();
    }
    // add two times
    invertedList = InvertedIndexManager.createOrOpen(path, analyzer);
    for (Document document : documents) {
      invertedList.addDocument(document);
    }

    invertedList.flush();
    for (Document document : documents) {
      invertedList.addDocument(document);
    }
    invertedList.flush();
  }

  // test if multiple keywords work or not. And we set 5 as a threshold for write counter and read
  // counter,
  // because I think the number will increase when we call the flush() function and we dont know the
  // execution order
  // of test cases, so we set them all to 5.
  @Test
  public void Test1() throws Exception {

    invertedList.deleteDocuments("pig");
    Set<Document> docSet = new HashSet<>();
    Iterator<Document> it = invertedList.documentIterator();
    //    Iterator<Document> qit = invertedList.searchQuery("cat");
    Integer resultCounter = 0;
    while (it.hasNext()) {
      assertFalse(it.next().getText().contains("pig"));
      resultCounter++;
    }

    // document can not find pig even we do not really delete it before merge
    assertEquals((int) 10, (int) resultCounter);

    Iterator<Document> its = invertedList.searchQuery("pig");
    assertFalse(its.hasNext());
  }
  // Test merge after merge all doc will not have pig
  @Test
  public void Test2() throws Exception {
    invertedList.deleteDocuments("pig");
    invertedList.mergeAllSegments();

    Iterator<Document> it = invertedList.documentIterator();
    while (it.hasNext()) {
      assertFalse(it.next().getText().contains("pig"));
    }
  }

  // Test the correctness of query after delete
  @Test
  public void Test3() throws Exception {
    invertedList.deleteDocuments("pig");
    Iterator<Document> its = invertedList.searchQuery("pig");
    assertFalse(its.hasNext());
    invertedList.deleteDocuments("toy");
    Iterator<Document> andit = invertedList.searchAndQuery(Arrays.asList("pig", "toy"));
    assertFalse(andit.hasNext());

    Iterator<Document> andor = invertedList.searchOrQuery(Arrays.asList("pig", "dog"));
    while (andor.hasNext()) {
      String text = andor.next().getText();
      System.out.println(text);
      assertTrue((text.contains("cat") || text.contains("Dot")));
    }
  }

  @After
  public void deleteTmp() throws Exception {
    PageFileChannel.resetCounters();
    Path rootPath = Paths.get(path);
    Files.walk(rootPath).sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);


  }
}
