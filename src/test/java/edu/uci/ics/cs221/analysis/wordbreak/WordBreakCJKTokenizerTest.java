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

        List<String> expected = Arrays.asList("今日", "学校", "行き", "ま", "す");

        WordBreakCJKTokenizer tokenizer = new WordBreakCJKTokenizer("JP");

        assertEquals(expected, tokenizer.tokenize(text));


    }


    @Test
    public void test2() {
        String text = "今日とても悲しい";
        List<String> expected = Arrays.asList("今日", "とても", "悲し", "い");
        WordBreakCJKTokenizer tokenizer = new WordBreakCJKTokenizer("JP");
        assertEquals(expected, tokenizer.tokenize(text));

    }

    @Test
    public void test3() {
        String text = "複数の言語を即座に処理することができ";
        List<String> expected = Arrays.asList("複数", "言語", "即座", "処理", "する", "こと", "でき");
        WordBreakCJKTokenizer tokenizer = new WordBreakCJKTokenizer("JP");
        assertEquals(expected, tokenizer.tokenize(text));

    }

    @Test
    public void test4() {
        String text = "sdslasldshkldsaldsald";//test for the exception
        try {
            WordBreakCJKTokenizer tokenizer = new WordBreakCJKTokenizer("JP");
        } catch (Exception e) {
            final String expected = "No possible way to break the word";
            assertEquals(expected, e.getMessage());
        }
    }

    @Test
    public void test5() throws Exception {
        WordBreakCJKTokenizer tokenizer = new WordBreakCJKTokenizer("CN");
        String s = "去北京大学玩";
        List<String> expected = Arrays.asList("去", "北京大学", "玩");
        long startTime = System.currentTimeMillis();   //获取开始时间
        List<String> r = tokenizer.tokenize(s);
        long endTime = System.currentTimeMillis(); //获取结束时间
        System.out.println("run time： " + (endTime - startTime) + "ms");
        assertEquals(expected, r);

    }

    @Test
    public void test6() throws Exception {
        WordBreakCJKTokenizer tokenizer = new WordBreakCJKTokenizer("CN");
        String s = "支持四种不同的东亚语言";
        List<String> expected = Arrays.asList("支持", "四种", "不同", "的", "东亚", "语言");
        long startTime = System.currentTimeMillis();   //获取开始时间
        List<String> r = tokenizer.tokenize(s);
        long endTime = System.currentTimeMillis(); //获取结束时间
        System.out.println("run time： " + (endTime - startTime) + "ms");
        assertEquals(expected, r);

    }
}