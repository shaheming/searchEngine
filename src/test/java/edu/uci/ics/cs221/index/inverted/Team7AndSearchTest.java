package edu.uci.ics.cs221.index.inverted;

import edu.uci.ics.cs221.analysis.Analyzer;
import edu.uci.ics.cs221.analysis.NaiveAnalyzer;
import edu.uci.ics.cs221.storage.Document;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class Team7AndSearchTest {
  private InvertedIndexManager manager;
  private static Document doc = new Document("cat dog monkey");
  private static Document doc1 = new Document("hello world");
  private static Document doc2 = new Document("cat dog ");
  String PATH = "./index/Team7AndSearchTest";

  @Before
  public void before() {
    Analyzer analyzer = new NaiveAnalyzer();
    File INDEX = new File(PATH);
    if (!INDEX.exists()) {
      INDEX.mkdirs();
    }
    manager = InvertedIndexManager.createOrOpen(PATH, analyzer);
    manager.addDocument(doc);
    manager.addDocument(doc1);
    manager.addDocument(doc2);
    manager.flush();
  }

  @After
  public void after() throws Exception {
    Path rootPath = Paths.get(PATH);
    Files.walk(rootPath).sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);


  }

  @Test
  /** Test when all the inputs qualify the AND operation */
  public void rightAndRight() {
    List<Document> expected = Arrays.asList(doc1);

    List<String> input = Arrays.asList("hello", "world");

    Iterator<Document> results = manager.searchAndQuery(input);

    int count = 0;
    while (results.hasNext()) {
      assertEquals(results.next().getText(), expected.get(count++).getText());
    }
  }

  @Test
  /** Test when there are multiple result for the search query */
  public void duplicateRightAndRight() {
    List<Document> expected = Arrays.asList(doc, doc2);

    List<String> input = Arrays.asList("cat", "dog");

    Iterator<Document> results = manager.searchAndQuery(input);

    int count = 0;
    while (results.hasNext()) {
      assertEquals(results.next().getText(), expected.get(count++).getText());
    }
  }

  @Test
  /** Test when the input search wordkey words are not in the inverted index */
  public void wrongAndWrong() {
    List<String> input = Arrays.asList("g", "g");

    Iterator<Document> results = manager.searchAndQuery(input);
    assertFalse(results.hasNext());
  }
}
