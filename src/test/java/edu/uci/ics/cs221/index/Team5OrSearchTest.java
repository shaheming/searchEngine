package edu.uci.ics.cs221.index;

import edu.uci.ics.cs221.analysis.Analyzer;
import edu.uci.ics.cs221.analysis.ComposableAnalyzer;
import edu.uci.ics.cs221.analysis.PorterStemmer;
import edu.uci.ics.cs221.analysis.WordBreakTokenizer;
import edu.uci.ics.cs221.index.inverted.InvertedIndexManager;
import edu.uci.ics.cs221.storage.Document;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.*;

public class Team5OrSearchTest {
    private InvertedIndexManager invertlist;
    private ArrayList<Document> docs;

    @Before
    public void setUp() throws Exception {
        Analyzer analyzer = new ComposableAnalyzer(new WordBreakTokenizer(), new PorterStemmer());
        invertlist = InvertedIndexManager.createOrOpen("src/test/java/edu/uci/ics/cs221/index/Team5OrSearchTest", analyzer);
        docs.add(new Document("cat dog toy"));
        docs.add(new Document("cat Dot"));
        docs.add(new Document("cat dot toy"));
        docs.add(new Document("cat toy Dog"));
        docs.add(new Document(""));
        docs.add(new Document("cat Dog"));


        //todo add more docs
    }


    //test if query find correct answer
    @Test
    public void searchOrQuery() throws Exception {

        List<String> strs = new ArrayList<>();
        strs.add("cat");
        strs.add("dog");
        Iterator<Document> iterator = invertlist.searchOrQuery(strs);
        while (iterator.hasNext()) {
            String text = iterator.next().getText();
            assertEquals(true, text.contains("dog") || text.contains("cat"));
        }

    }


    @After
    public void deletetmp() throws Exception{
        docs.clear();
        invertlist.flush();


    }
    //todo test if the query find all matched docs
    //todo delete tmp file after test

}