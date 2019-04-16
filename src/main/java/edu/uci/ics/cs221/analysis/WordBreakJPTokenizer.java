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
public class WordBreakJPTokenizer implements Tokenizer {
    public static Map<String, Double> wordDict;
    public static Double totalWords;
    private Double maxP = Double.MIN_VALUE;
    private LinkedList<List<Integer>> maxPath;


    public WordBreakJPTokenizer() {
        try {
            // load the dictionary corpus
            URL dictResource = WordBreakJPTokenizer.class.getClassLoader().getResource("cs221_frequency_dictionary_en1.txt");
//            System.out.println(dictResource);
            List<String> dictLines = Files.readAllLines(Paths.get(dictResource.toURI()));
//System.out.println("dictLines size: " + dictLines.size());
            //System.out.println(dictLines.get(10));

            wordDict = new HashMap<>();
            Double wordsCount = 0.0;
            for (String line : dictLines) {
                String[] col = line.split("\\s");
                Double freq = Double.valueOf(col[1]);
                wordsCount += freq;
                wordDict.put(col[0], freq);
                for (int i = 0; i < col[0].length(); i++) {
                    if (!wordDict.containsKey(col[0].substring(0, i + 1))) {
                        wordDict.put(col[0].substring(0, i + 1), 0.0);
                    }
                }
            }
            totalWords = wordsCount;
            maxPath = new LinkedList<>();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public List<List<Integer>> getDAG(String s) {

        List<List<Integer>> DAG = new ArrayList<>();
        for (int i = 0; i < s.length(); i++) {
            DAG.add(new ArrayList<>());
            int j = i;
            String frag = s.substring(i, i + 1);
            while (j < s.length() && wordDict.containsKey(frag)) {
                if (wordDict.get(frag) > 0) {
                    DAG.get(i).add(j + 1);
                }
                j++;
                if (j < s.length()) {
                    frag = s.substring(i, j + 1);
                }

            }
        }
        return DAG;
    }

    public List<String> tokenize(String text) {

        if (text.length() == 0) {
            throw new RuntimeException("there's no possible way to break the string");
        }
        //some edge condition?
        List<List<Integer>> dag = getDAG(text);
        LinkedList<String> tokenizedWords = new LinkedList<>();
        int[] pres = findMaxPath(text, dag);
        int pre = dag.size();
//        System.out.println(Arrays.toString(pres));
        while (pre != pres[pre]) {
//            System.out.println(text.substring(pres[pre], pre));
            if (!StopWords.stopWords.contains(text.substring(pres[pre], pre))) {
                tokenizedWords.addFirst(text.substring(pres[pre], pre));
            }
            pre = pres[pre];
        }
        return new ArrayList<>(tokenizedWords);
    }


    private int[] findMaxPath(String text, List<List<Integer>> dag) {
        List<List<Integer>> path = new ArrayList<>();

        //Longest Path in a Directed Acyclic Graph
        //dist[v] = max{dist[pre] + weight(pre->v)};
        //w1 -> w2 -> w3 -> w4 -> end
        //edge w1 -> w2 represent the frequency of words w1 in dict
        double[] dist = new double[dag.size() + 1];
        int[] pre = new int[dag.size() + 1];
        Arrays.fill(dist, 0.0);
        dist[0] = 1.0;
        pre[0] = 0;
        for (int i = 0; i < dag.size(); i++) {
            for (int j : dag.get(i)) {
                double p = wordDict.get(text.substring(i, j)) / totalWords;
                if (dist[i] * p > dist[j]) {
                    pre[j] = i;
                    dist[j] = dist[i] * p;
                }
            }
        }
        return pre;
    }
}
