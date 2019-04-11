package edu.uci.ics.cs221.analysis.wordbreak;


import edu.uci.ics.cs221.analysis.WordBreakTokenizer;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class WordBreakTokenizerTest {

    @Test
    public void test1() {

        String text = "cattodog";
        List<String> expected = Arrays.asList("cat", "dog");
        WordBreakTokenizer tokenizer = new WordBreakTokenizer();
        long startTime=System.currentTimeMillis();   //获取开始时间
        List<String> r = tokenizer.tokenize(text);
        long endTime=System.currentTimeMillis(); //获取结束时间
        System.out.println("run time： "+(endTime-startTime)+"ms");
        System.out.println((double) startTime/(double)endTime);
        assertEquals(expected,r );

    }

}
