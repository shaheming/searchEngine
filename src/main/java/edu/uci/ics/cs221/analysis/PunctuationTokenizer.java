package edu.uci.ics.cs221.analysis;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.ArrayList;

/**
 * Project 1, task 1: Implement a simple tokenizer based on punctuations and white spaces.
 * <p>
 * For example: the text "I am Happy Today!" should be tokenized to ["happy", "today"].
 * <p>
 * Requirements:
 * - White spaces (space, tab, newline, etc..) and punctuations provided below should be used to tokenize the text.
 * - White spaces and punctuations should be removed from the result tokens.
 * - All tokens should be converted to lower case.
 * - Stop words should be filtered out. Use the stop word list provided in `StopWords.java`
 */
public class PunctuationTokenizer implements Tokenizer {

    public static Set<String> punctuations = new HashSet<>();

    static {
        punctuations.addAll(Arrays.asList(",", ".", ";", "?", "!"));
    }

    public PunctuationTokenizer() {
    }

    public List<String> tokenize(String text) {


        char[] chars = text.toCharArray();

        for (int i = 0; i < text.length(); i++) {
            if (punctuations.contains(String.valueOf(chars[i]))) {
                chars[i] = ' ';
            }
        }

        List<String> strings = new ArrayList<>(Arrays.asList(String.valueOf(chars).toLowerCase().split("\\s+")));

        strings.removeIf(item -> ("".equals(item)));

        List<String> res = new ArrayList<>();
        for (String s : strings) {
            if (!StopWords.stopWords.contains(s)) {
                res.add(s);
            }
        }
        return res;

    }

}
