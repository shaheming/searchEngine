package edu.uci.ics.cs221.analysis.wordbreak;


import edu.uci.ics.cs221.analysis.WordBreakTokenizerSha;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class WordBreakTokenizerShaTest {

    @Test
    public void test1() {

        String text = "cattodog";
        List<String> expected = Arrays.asList("cat", "dog");
        WordBreakTokenizerSha tokenizer = new WordBreakTokenizerSha();
        assertEquals(expected, tokenizer.tokenize(text));

    }
    @Test
    public void test2() {
        String text = "something";

        List<String> expected = Arrays.asList( "something");

        WordBreakTokenizerSha tokenizer = new WordBreakTokenizerSha();

        assertEquals(expected, tokenizer.tokenize(text));




    }

    @Test
    public void test5() {
        String text = "sdslasldshkldsaldsald";
//test for the exception
        List<String> expected = Arrays.asList( "");

        WordBreakTokenizerSha tokenizer = new WordBreakTokenizerSha();

        assertEquals(expected, tokenizer.tokenize(text));




    }


}
