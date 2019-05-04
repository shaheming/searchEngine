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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

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
    while (it.hasNext()) {
      assertFalse(it.next().getText().contains("pig"));
    }

    invertedList.mergeAllSegments();
    while (it.hasNext()) {
      assertFalse(it.next().getText().contains("pig"));
    }
    Iterator<Document> its = invertedList.searchQuery("pig");

    assertFalse(its.hasNext());
    invertedList.deleteDocuments("toi");
    Iterator<Document> andit = invertedList.searchAndQuery(Arrays.asList("pig","toy"));
    assertFalse(andit.hasNext());

    Iterator<Document> andor = invertedList.searchOrQuery(Arrays.asList("pig","dog"));
    while (andor.hasNext()) {
      String text =andor.next().getText();
      System.out.println(text);
      assertTrue((text.contains("cat") || text.contains("Dot")) );
    }

  }

  @After
  public void deleteTmp() throws Exception {
    PageFileChannel.resetCounters();
    Path rootPath = Paths.get(path);
    Files.walk(rootPath).sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);

    Files.deleteIfExists(rootPath);
  }
}
