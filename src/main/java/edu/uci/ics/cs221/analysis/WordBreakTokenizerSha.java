package edu.uci.ics.cs221.analysis;

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

// * - A match in dictionary is case insensitive. Output tokens should all be in lower case.
// * - Stop words should be removed.
// * - If there's no possible way to break the string, throw an exception.
public class WordBreakTokenizerSha implements Tokenizer {
    public Map<String, Double> wordDict;
    private Double maxP = Double.MIN_VALUE;
    private LinkedList<List<Integer>> maxPath;
    private Double wordsCount = 0.0;

    public WordBreakTokenizerSha() {
        try {
            // load the dictionary corpus
            URL dictResource = WordBreakTokenizerSha.class.getClassLoader().getResource("cs221_frequency_dictionary_en.txt");
            System.out.println(dictResource);
            List<String> dictLines = Files.readAllLines(Paths.get(dictResource.toURI()));
            System.out.println(dictLines.get(0));

            wordDict = new HashMap<>();

            for (String line : dictLines) {
                if (line.startsWith("\uFEFF")) {
                    line = line.substring(1);
                }
                String[] col = line.split("\\s");
                Double freq = Double.valueOf(col[1]);
                wordsCount += freq;
                wordDict.put(col[0], freq);
            }
//            for (Map.Entry<String, Double> entry : wordDict.entrySet()) {
//                Double freq = entry.getValue() / wordsCount;
//                wordDict.put(entry.getKey(), freq);
//            }
            maxPath = new LinkedList<>();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public List<String> tokenize(String text) {

        if (text.length() == 0) {
            //TODO(all): find a better exception
            throw new RuntimeException("there's no possible way to break the string");
        }

        String lowerText = text.toLowerCase();
        System.out.println(lowerText);
        boolean[][] wordMatrix = new boolean[lowerText.length()][lowerText.length()];
        for (int i = 0; i < lowerText.length(); i++) {
            Arrays.fill(wordMatrix[i], false);
        }
        //catdog
        for (int length = 1; length <= lowerText.length(); length++) {
            for (int start = 0; start < lowerText.length() - length + 1; start++) {
                int end = start + length - 1;

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
        int len = wordMatrix.length;
        if (!wordMatrix[0][len - 1]) {
            throw new RuntimeException("there's no possible way to break the string");
        }

        List<String> tokenizedWords = new ArrayList<>();
        //System.out.println(wordDict.get("dog"));

        LinkedList<List<Integer>> path = new LinkedList<>();
        findMaxPath(wordMatrix, lowerText, 0, 1, path);

        for (List<Integer> temp : maxPath) {
            //System.out.println(lowerText.substring(temp.get(0), temp.get(1) + 1));
            String word = lowerText.substring(temp.get(0), temp.get(1) + 1);
            //filter stop words
            if (!StopWords.stopWords.contains(word)) {
                tokenizedWords.add(word);
            }
        }
        return tokenizedWords;
    }

    //dfs
    private void findMaxPath(boolean[][] wordMatrix, String text, int start, double p, LinkedList<List<Integer>> path) {
        //start = row, end = col
        //if end == col that means we go to check the last words

        if (start >= wordMatrix.length) {

            if (p > maxP) {
                maxPath = (LinkedList) path.clone();
                maxP = p;
            }
            return;
        }

        for (int end = start; end < wordMatrix.length; end++) {
            if (wordMatrix[start][end] && wordDict.containsKey(text.substring(start, end + 1))) {
                path.add(Arrays.asList(start, end));
                findMaxPath(wordMatrix, text, end + 1, p * wordDict.get(text.substring(start, end + 1)) / wordsCount, path);
                path.pollLast();
            }
        }
    }
}
