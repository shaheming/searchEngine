package edu.uci.ics.cs221.analysis;

import java.lang.reflect.Array;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

/**
 * Project 1, task 2: Implement a Dynamic-Programming based Word-Break Tokenizer.
 * <p>
 * Word-break is a problem where given a dictionary and a string (text with all white spaces removed),
 * determine how to break the string into sequence of words.
 * For example:
 * input string "catanddog" is broken to tokens ["cat", "and", "dog"]
 * <p>
 * We provide an English dictionary corpus with frequency information in "resources/cs221_frequency_dictionary_en.txt".
 * Use frequency statistics to choose the optimal way when there are many alternatives to break a string.
 * For example,
 * input string is "ai",
 * dictionary and probability is: "a": 0.1, "i": 0.1, and "ai": "0.05".
 * <p>
 * Alternative 1: ["a", "i"], with probability p("a") * p("i") = 0.01
 * Alternative 2: ["ai"], with probability p("ai") = 0.05
 * Finally, ["ai"] is chosen as result because it has higher probability.
 * <p>
 * Requirements:
 * - Use Dynamic Programming for efficiency purposes.
 * - Use the the given dictionary corpus and frequency statistics to determine optimal alternative.
 * The probability is calculated as the product of each token's probability, assuming the tokens are independent.
 * - A match in dictionary is case insensitive. Output tokens should all be in lower case.
 * - Stop words should be removed.
 * - If there's no possible way to break the string, throw an exception.
 */
public class WordBreakTokenizer implements Tokenizer {
    public Map<String, Double> wordDict;
    private Double maxP = Double.MIN_VALUE;
    private LinkedList<List<Integer>> maxPath;


    public WordBreakTokenizer() {
        try {
            // load the dictionary corpus
            URL dictResource = WordBreakTokenizer.class.getClassLoader().getResource("cs221_frequency_dictionary_en.txt");
            System.out.println(dictResource);
            List<String> dictLines = Files.readAllLines(Paths.get(dictResource.toURI()));
            System.out.println("dictLines size: " + dictLines.size());
            System.out.println(dictLines.get(0));

            wordDict = new HashMap<>();
            Double wordsCount = 0.0;
            for (String line : dictLines) {
                String[] col = line.split("\\s");
                Double freq = Double.valueOf(col[1]);
                wordsCount += freq;
                wordDict.put(col[0], freq);
            }
            for (Map.Entry<String, Double> entry : wordDict.entrySet()) {
                Double freq = entry.getValue() / wordsCount;
                wordDict.put(entry.getKey(), freq);
            }
            maxPath = new LinkedList<>();
            //todo delete test
//            maxPath.add(Arrays.asList(1, 2));
            System.out.println(maxPath.getFirst());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public List<String> tokenize(String text) {
        //opt[start][end] = any(opt[start][k] && opt[k+1][end])  start<= k <end
        String lowerText = text.toLowerCase();

        boolean[][] wordMatrix = new boolean[lowerText.length()][lowerText.length()];
        for (int i = 0; i < lowerText.length(); i++) {
            Arrays.fill(wordMatrix[i], false);
        }

        List<String> tokenizedWords = new ArrayList<>();
        //catdog
        for (int length = 1; length <= lowerText.length(); length++) {
            for (int start = 0; start < lowerText.length() - length + 1; start++) {
                int end = start + length - 1;
                //todo delete test
                System.out.println(lowerText.substring(start, end + 1));
                if (wordDict.containsKey(lowerText.substring(start, end + 1))) {
                    wordMatrix[start][end] = true;
                }
                for (int k = start; k < end; k++) {
                    if (wordMatrix[start][k] && wordMatrix[k + 1][end]) {
                        wordMatrix[start][end] = true;
                    }
                }
            }
        }


        System.out.println(wordDict.get("dog"));
        System.out.println(Arrays.deepToString(wordMatrix));
        return tokenizedWords;
    }

    public void findMaxPath(boolean[][] wordMatrix, String text, int start, int end, double p, LinkedList<List<Integer>> path) {
        //start = row, end = col
        //if end == col that means we go to check the last words
        if (start == wordMatrix.length) {
            if (wordDict.containsKey(text.substring(start, start + 1))) {
                p *= wordDict.get(text.substring(start, start + 1));
                if (p > maxP) {
                    path.add(Arrays.asList(start, end));
                    maxPath = (LinkedList) path.clone();
                    path.pollLast();
                }
            }
        }

        for (int e = start; e < wordMatrix.length; e++) {
            if (wordMatrix[start][e] && wordDict.containsKey(text.substring(start, e + 1))) {
                path.add(Arrays.asList(start, e));
                findMaxPath(wordMatrix, text, e+1, e+1, p * wordDict.get(text.substring(start, e + 1)), path);
                path.pollLast();
            }
        }
    }
}
