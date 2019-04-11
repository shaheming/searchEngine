package edu.uci.ics.cs221.analysis.wordbreak;

import edu.uci.ics.cs221.analysis.WordBreakCNTokenizer;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

public class WordBreakCNTokenizerTest {
    @Test
    public void test1() throws Exception {
        WordBreakCNTokenizer tokenizer = new WordBreakCNTokenizer();
        String s = "去北京大学玩";
        List<String> expected = Arrays.asList("去", "北京大学","玩");
        long startTime=System.currentTimeMillis();   //获取开始时间
        List<String> r = tokenizer.tokenize(s);
        long endTime=System.currentTimeMillis(); //获取结束时间
        System.out.println("run time： "+(endTime-startTime)+"ms");
        assertEquals(expected,r );

    }
    @Test
    public void test2() throws Exception {
        WordBreakCNTokenizer tokenizer = new WordBreakCNTokenizer();
        String s = "去";
        List<String> expected = Arrays.asList("去");
        long startTime=System.currentTimeMillis();   //获取开始时间
        List<String> r = tokenizer.tokenize(s);
        long endTime=System.currentTimeMillis(); //获取结束时间
        System.out.println("run time： "+(endTime-startTime)+"ms");
        assertEquals(expected,r );

    }

//    @Test
//    public void test3() throws Exception {
//        WordBreakCNTokenizer tokenizer = new WordBreakCNTokenizer();
//
//        List<String> expected = Arrays.asList("");
//        long startTime=System.currentTimeMillis();   //获取开始时间
//        List<String> r = tokenizer.tokenize(s);
//        long endTime=System.currentTimeMillis(); //获取结束时间
//        System.out.println("run time： "+(endTime-startTime)+"ms");
//        assertEquals(expected,r );
//
//    }


}