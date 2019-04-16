package edu.uci.ics.cs221.analysis.wordbreak;

import edu.uci.ics.cs221.analysis.WordBreakJPTokenizer;
import edu.uci.ics.cs221.analysis.WordBreakTokenizer;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

public class WordBreakJPTokenizerTest {

    @Test
    public void test1() {
        String text = "今日は学校に行きます";

        List<String> expected = Arrays.asList("今日", "学校", "行き", "ま","す");

        WordBreakJPTokenizer tokenizer = new WordBreakJPTokenizer();

        assertEquals(expected, tokenizer.tokenize(text));


    }


    @Test
    public void test2() {
        String text = "今日とても悲しい";

        List<String> expected = Arrays.asList("今日","とても","悲し","い");
//test stop words
        WordBreakJPTokenizer tokenizer = new WordBreakJPTokenizer();
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
            WordBreakJPTokenizer tokenizer = new WordBreakJPTokenizer();
        } catch (Exception e) {
            final String expected = "No possible way to break the word";
            assertEquals(expected, e.getMessage());
        }
    }
}