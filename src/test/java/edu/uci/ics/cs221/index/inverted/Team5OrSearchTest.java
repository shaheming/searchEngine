package edu.uci.ics.cs221.index.inverted;

import edu.uci.ics.cs221.analysis.Analyzer;
import edu.uci.ics.cs221.analysis.ComposableAnalyzer;
import edu.uci.ics.cs221.analysis.PorterStemmer;
import edu.uci.ics.cs221.analysis.PunctuationTokenizer;
import edu.uci.ics.cs221.storage.Document;
import edu.uci.ics.cs221.storage.DocumentStore;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static edu.uci.ics.cs221.storage.MapdbDocStore.createOrOpen;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class Team5OrSearchTest {
    private String path="src/test/java/edu/uci/ics/cs221/index";
    private DocumentStore documentStore = createOrOpen(path+"/test.db");
    private Analyzer analyzer = new ComposableAnalyzer(new PunctuationTokenizer(), new PorterStemmer());
    private InvertedIndexManager invertedList = InvertedIndexManager.createOrOpen(path, analyzer);

    @Before
    public void setUp() throws Exception {
        documentStore.addDocument(0, new Document("cat dog toy"));
        documentStore.addDocument(1, new Document("cat Dot"));
        documentStore.addDocument(2, new Document("cat dot toy"));
        documentStore.addDocument(3, new Document("cat toy Dog"));
        documentStore.addDocument(4, new Document("toy dog cat"));
        documentStore.addDocument(5, new Document("cat Dog"));//docs cannot be null

        for (int i = 0; i < documentStore.size(); i++) {
            invertedList.addDocument(documentStore.getDocument(i));
            invertedList.flush();
        }
    }

    //test if multiple keywords work or not
    @Test
    public void Test1() throws Exception {
        List<String> words = new ArrayList<>();
        words.add("cat");
        words.add("dog");

        Iterator<Document> iterator = invertedList.searchOrQuery(words);
        int counter = 0;
        while (iterator.hasNext()) {
            String text = iterator.next().getText();
            assertEquals(true, text.contains("dog") || text.contains("cat"));
            counter++;
        }
        assertEquals(6, counter);
        assertTrue(PageFileChannel.readCounter >= 20 && PageFileChannel.writeCounter >= 20);
        words.clear();

    }

    //test if single key words works or not    
    @Test
    public void Test2() throws Exception {
        List<String> words = new ArrayList<>();
        words.add("dog");

        Iterator<Document> iterator = invertedList.searchOrQuery(words);
        int counter = 0;
        while (iterator.hasNext()) {
            String text = iterator.next().getText();
            assertEquals(true, text.contains("dog"));
            counter++;

        }
        assertEquals(4, counter);
        assertTrue(PageFileChannel.readCounter >= 20 && PageFileChannel.writeCounter >= 20);
        words.clear();

    }

    //test the case that the key word does not match any file
    @Test
    public void Test3() throws Exception {
        List<String> words = new ArrayList<>();
        words.add("sdasjdlslsah");
        Iterator<Document> iterator = invertedList.searchOrQuery(words);
        int counter = 0;
        while (iterator.hasNext()) {

            String text = iterator.next().getText();
            assertEquals(true, text.contains("sdasjdlslsah"));
            counter++;

        }
        assertEquals(0, counter);
        assertTrue(PageFileChannel.readCounter >= 20 && PageFileChannel.writeCounter >= 20);
        words.clear();

    }

    //test or operation works or not
    @Test
    public void Test4() throws Exception {
        List<String> words = new ArrayList<>();
        words.add("toy");
        words.add("dog");
        Iterator<Document> iterator = invertedList.searchOrQuery(words);
        int counter = 0;
        while (iterator.hasNext()) {

            String text = iterator.next().getText();
            assertEquals(true, text.contains("dog") || text.contains("toy"));
            counter++;

        }
        assertEquals(5, counter);
        assertTrue(PageFileChannel.readCounter >= 20 && PageFileChannel.writeCounter >= 20);
        words.clear();

    }


    @After
    public void deleteTmp() throws Exception {
        if (documentStore != null) documentStore.close();
        PageFileChannel.resetCounters();
        Files.deleteIfExists(Paths.get(path+"/test.db"));
    }

}