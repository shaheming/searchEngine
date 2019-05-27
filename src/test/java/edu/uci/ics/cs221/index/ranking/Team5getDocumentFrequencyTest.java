package edu.uci.ics.cs221.index.ranking;
import edu.uci.ics.cs221.analysis.Analyzer;
import edu.uci.ics.cs221.analysis.ComposableAnalyzer;
import edu.uci.ics.cs221.analysis.PorterStemmer;
import edu.uci.ics.cs221.analysis.PunctuationTokenizer;
import edu.uci.ics.cs221.index.inverted.InvertedIndexManager;
import edu.uci.ics.cs221.index.inverted.PageFileChannel;
import edu.uci.ics.cs221.storage.Document;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import java.io.File;
import java.util.List;

import static org.junit.Assert.*;

public class Team5getDocumentFrequencyTest {
    private String path = "./index/Team5getDocumentFrequencyTest";
    private Analyzer analyzer = new ComposableAnalyzer(new PunctuationTokenizer(), new PorterStemmer());
    private InvertedIndexManager invertedList;

    @Before
    public void setUp() throws Exception {
        File directory = new File(path);
        if (!directory.exists()) {
            directory.mkdirs();
        }
        invertedList = InvertedIndexManager.createOrOpen(path, analyzer);
        invertedList.addDocument(new Document("cat dog toy"));
        invertedList.addDocument(new Document("cat Dot"));
        invertedList.addDocument(new Document("cat dot toy"));
        invertedList.flush();
        invertedList.addDocument(new Document("cat toy Dog"));
        invertedList.addDocument(new Document("toy dog cat"));
        invertedList.flush();
        invertedList.addDocument(new Document("cat Dog"));//docs cannot be null
        invertedList.flush();
    }

    // test multiple keywords with multiple segments
    @Test
    public void Test1() throws Exception {
        String words="cat dog Toy Dot";
        List<String> new_words=analyzer.analyze(words);
        int result=invertedList.getDocumentFrequency(0,new_words.get(0));
        assertEquals(3,result);
        result=invertedList.getDocumentFrequency(1,new_words.get(0));
        assertEquals(2,result);
        result=invertedList.getDocumentFrequency(2,new_words.get(0));
        assertEquals(1,result);

        result=invertedList.getDocumentFrequency(0,new_words.get(1));
        assertEquals(1,result);
        result=invertedList.getDocumentFrequency(1,new_words.get(1));
        assertEquals(2,result);
        result=invertedList.getDocumentFrequency(2,new_words.get(1));
        assertEquals(1,result);

        result=invertedList.getDocumentFrequency(0,new_words.get(2));
        assertEquals(2,result);
        result=invertedList.getDocumentFrequency(1,new_words.get(2));
        assertEquals(2,result);
        result=invertedList.getDocumentFrequency(2,new_words.get(2));
        assertEquals(0,result);

        result=invertedList.getDocumentFrequency(0,new_words.get(3));
        assertEquals(2,result);
        result=invertedList.getDocumentFrequency(1,new_words.get(3));
        assertEquals(0,result);
        result=invertedList.getDocumentFrequency(2,new_words.get(3));
        assertEquals(0,result);

    }
    //test the case that the key word does not match any file
    @Test
    public void Test2() throws Exception {
        String words="sdasjdlslsah";
        List<String> new_words=analyzer.analyze(words);
        int n=invertedList.getNumSegments();
        for(int i=0;i<new_words.size();i++){
            for(int j=0;j<n;j++) {
                int result=invertedList.getDocumentFrequency(j,new_words.get(i));
                assertEquals(0,result);
            }
        }
    }

    @After

    public void deleteTmp() throws Exception {
        PageFileChannel.resetCounters();
        File f = new File(path);
        File[] files = f.listFiles();
        for (File file : files) {
            file.delete();
        }
        f.delete();
    }

}
