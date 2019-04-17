package edu.uci.ics.cs221.analysis.wordbreak;

import edu.uci.ics.cs221.analysis.WordBreakCJKTokenizer;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

public class WordBreakCJKTokenizerTest {

    @Test
    public void test1() {
        String text = "今日は学校に行きます";

        List<String> expected = Arrays.asList("今日", "学校", "行き", "ま","す");

        WordBreakCJKTokenizer tokenizer = new WordBreakCJKTokenizer("JP");

        assertEquals(expected, tokenizer.tokenize(text));


    }


    @Test
    public void test2() {
        String text = "今日とても悲しい";

        List<String> expected = Arrays.asList("今日","とても","悲し","い");
//test stop words
        WordBreakCJKTokenizer tokenizer = new WordBreakCJKTokenizer("JP");
        //long startTime = System.currentTimeMillis();   //获取开始时间
       // List<String> r = tokenizer.tokenize(text);
        //long endTime = System.currentTimeMillis(); //获取结束时间
        //System.out.println("run time： " + (endTime - startTime) + "ms");
        //System.out.println((double) startTime / (double) endTime);
        assertEquals(expected, tokenizer.tokenize(text));

    }

    @Test
    public void test3() {
        String text = "sdslasldshkldsaldsald";//test for the exception
        try {
            WordBreakCJKTokenizer tokenizer = new WordBreakCJKTokenizer("JP");
        } catch (Exception e) {
            final String expected = "No possible way to break the word";
            assertEquals(expected, e.getMessage());
        }
    }

    @Test
    public void test4() throws Exception {
        WordBreakCJKTokenizer tokenizer = new WordBreakCJKTokenizer("CN");
        String s = "去北京大学玩";
        List<String> expected = Arrays.asList("去", "北京大学","玩");
        long startTime=System.currentTimeMillis();   //获取开始时间
        List<String> r = tokenizer.tokenize(s);
        long endTime=System.currentTimeMillis(); //获取结束时间
        System.out.println("run time： "+(endTime-startTime)+"ms");
        assertEquals(expected,r );

    }
    @Test
    public void test5() throws Exception {
        WordBreakCJKTokenizer tokenizer = new WordBreakCJKTokenizer("CN");
        String s = "去";
        List<String> expected = Arrays.asList("去");
        long startTime=System.currentTimeMillis();   //获取开始时间
        List<String> r = tokenizer.tokenize(s);
        long endTime=System.currentTimeMillis(); //获取结束时间
        System.out.println("run time： "+(endTime-startTime)+"ms");
        assertEquals(expected,r );

    }
}