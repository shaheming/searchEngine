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
        assertEquals(expected, tokenizer.tokenize(text));

    }
    @Test
    public void test2() {
        String text = "something";

        List<String> expected = Arrays.asList( "something");

        WordBreakTokenizer tokenizer = new WordBreakTokenizer();

        assertEquals(expected, tokenizer.tokenize(text));




    }
    @Test
    public void test3() {
        String text = "今日は学校に行きます";

        List<String> expected = Arrays.asList( "今日", "学校", "行き", "ます");

        WordBreakTokenizer tokenizer = new WordBreakTokenizer();

        assertEquals(expected, tokenizer.tokenize(text));




    }
    @Test
    public void test4() {
        String text = "私の鉛筆がなくなったので貸してもらえませんか";

        List<String> expected = Arrays.asList( "鉛筆", "なくな", "った", "ので", "貸し", "て", "もらえ", "ま", "せん", "か");
//test stop words
        WordBreakTokenizer tokenizer = new WordBreakTokenizer();

        assertEquals(expected, tokenizer.tokenize(text));




    }
    @Test
    public void test5() {
        String text = "sdslasldshkldsaldsald";
//test for the exception
        List<String> expected = Arrays.asList( "");

        WordBreakTokenizer tokenizer = new WordBreakTokenizer();

        assertEquals(expected, tokenizer.tokenize(text));




    }


}
