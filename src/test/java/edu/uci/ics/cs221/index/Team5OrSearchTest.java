package edu.uci.ics.cs221.index;

import edu.uci.ics.cs221.analysis.Analyzer;
import edu.uci.ics.cs221.analysis.ComposableAnalyzer;
import edu.uci.ics.cs221.analysis.PorterStemmer;
import edu.uci.ics.cs221.analysis.WordBreakTokenizer;
import edu.uci.ics.cs221.index.inverted.InvertedIndexManager;
import edu.uci.ics.cs221.storage.Document;
import edu.uci.ics.cs221.storage.DocumentStore;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import static edu.uci.ics.cs221.storage.MapdbDocStore.createOrOpen;
import static org.junit.Assert.*;

public class Team5OrSearchTest {
    private InvertedIndexManager invertlist;
    private DocumentStore documentStore=createOrOpen("./test.db");

    @Before
    public void setUp() throws Exception {
        Analyzer analyzer = new ComposableAnalyzer(new WordBreakTokenizer(), new PorterStemmer());
        invertlist = InvertedIndexManager.createOrOpen("src/test/java/edu/uci/ics/cs221/index/Team5OrSearchTest", analyzer);
        documentStore.addDocument(0,new Document("cat dog toy"));
        documentStore.addDocument(1,new Document("cat Dot"));
        documentStore.addDocument(2,new Document("cat dot toy"));
        documentStore.addDocument(3,new Document("cat toy Dog"));
        documentStore.addDocument(4,new Document(""));
        documentStore.addDocument(5,new Document("cat Dog"));


        //todo add more docs
    }


    //test if query find correct answer
    @Test
    public void Test1() throws Exception {


        List<String> strs = new ArrayList<>();
        List<Boolean> flag = new ArrayList<>();
        strs.add("cat");
        strs.add("dog");

        Iterator<Document> iterator = invertlist.searchOrQuery(strs);
        while (iterator.hasNext()) {
            String text = iterator.next().getText();
            if(text.contains("dog") || text.contains("cat")){
                flag.add(true);
            }
            else flag.add(false);
        }



        assertEquals(Arrays.asList(true,true,true,true),flag );
        flag.clear();

    }


    @Test
    public void Test2() throws Exception {


        List<String> strs = new ArrayList<>();
        List<Boolean> flag = new ArrayList<>();
        strs.add("cat");
        strs.add("dog");

        Iterator<Document> iterator = invertlist.searchOrQuery(strs);
        while (iterator.hasNext()) {
            String text = iterator.next().getText();
            if(text.contains("dog") || text.contains("cat")){
                flag.add(true);
            }
            else flag.add(false);
        }



        assertEquals(Arrays.asList(true,true,true,true),flag );
        flag.clear();

    }


    @After
    public void deletetmp() throws Exception{
        documentStore.close();
        invertlist.flush();
        Files.deleteIfExists(Paths.get("./test.db"));

    }
    //todo test if the query find all matched docs


}