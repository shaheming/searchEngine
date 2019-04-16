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

// * - A match in dictionary is case insensitive. Output tokens should all be in lower case.
// * - Stop words should be removed.
// * - If there's no possible way to break the string, throw an exception.
public class WordBreakTokenizer implements Tokenizer {
    private  List<String> dictLines;
    private  List<String> maxpath=new LinkedList<>();
    private  Map<String,Double> map=new HashMap<String, Double>();
    private  double tmp_double=0;
    public WordBreakTokenizer() {
        try {
            // load the dictionary corpus
            URL dictResource = WordBreakTokenizer.class.getClassLoader().getResource("dict_jp.txt");
            dictLines = Files.readAllLines(Paths.get(dictResource.toURI()));
           // System.out.println(dictLines.get(1));
            double sum=0;
            for(int i=0;i<dictLines.size();i++){
                sum=sum+Double.parseDouble(dictLines.get(i).split(" ")[1]);
            }
            for(int i=0;i<dictLines.size();i++){
                Double tmp=Double.valueOf(Double.parseDouble(dictLines.get(i).split(" ")[1])/sum);
                map.put(dictLines.get(i).split(" ")[0],tmp);
            }

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public List<String> tokenize(String text) {
        text=text.toLowerCase();
        List<String> res = new LinkedList<>();
        int n=text.length();
        boolean str[][]=new boolean[n][n];
        for(int j=0;j<n;j++){
            for(int i=j;i>=0;i--) {

             check(i,j,text,str); //res.add(text.substring(i,j+1));
              //System.out.print(str[i][j]);
            }
        }
        if(!str[0][n-1]) throw new UnsupportedOperationException("No possible way to break the word");
        mostlikepath(str,text,0,res,n,1);
        res.clear();
        for(int i=0;i<maxpath.size();i++){
            if (!StopWords.stopWords.contains(maxpath.get(i))) {
               res.add(maxpath.get(i));
            }
        }
        return res;
    }

    boolean check(int start, int end, String text,boolean str[][]){
        if(end<start) {
            str[start][end]=false;
            return false;
        }
        String tmp=text.substring(start,end+1);

        for(int i=0;i<dictLines.size();i++){
            if(map.containsKey(tmp)) {
                str[start][end]=true;

                return true;
            }
        }

        for(int i=start;i<=end;i++){
            if(str[start][i]&&str[i+1][end]) {
                str[start][end]=true;

                return false;
            }

        }
        str[start][end]=false;
        return false;
    }

     void mostlikepath(boolean str[][],String text,int pointer,List<String> path,int n,double p){
         if(pointer==text.length()){
             if(p>=tmp_double) {
                 tmp_double = p;
                 maxpath.clear();
                 for(int i=0;i<path.size();i++) maxpath.add(path.get(i));

             }
             return;
         }
        for(int s=pointer;s<n;s++){
            if(str[pointer][s]&&map.containsKey(text.substring(pointer,s+1))){
                //System.out.println(text.substring(pointer,s+1));
                path.add(text.substring(pointer,s+1));
                mostlikepath(str,text,s+1,path,n,p*map.get(text.substring(pointer,s+1)));
                //System.out.println(path.get(path.size()-1));
                path.remove(path.size()-1);

            }


        }
        return ;
    }


}