package edu.uci.ics.cs221.search;

import com.google.common.base.Preconditions;
import edu.uci.ics.cs221.index.inverted.InvertedIndexManager;
import edu.uci.ics.cs221.index.inverted.Pair;
import edu.uci.ics.cs221.storage.Document;
import no.uib.cipr.matrix.DenseVector;
import no.uib.cipr.matrix.sparse.CompRowMatrix;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

public class IcsSearchEngine {
  private InvertedIndexManager indexManager;
  private Path documentDirectory;
  private double[] pageRankStore;

  private IcsSearchEngine(Path documentDirectory, InvertedIndexManager indexManager) {
    File dir = documentDirectory.toFile();
    if (!dir.exists()) {
      throw new RuntimeException("Can not read documents");
    }
    this.indexManager = indexManager;
    this.documentDirectory = documentDirectory;
  }

  /** Initializes an IcsSearchEngine from the directory containing the documents and the */
  public static IcsSearchEngine createSearchEngine(
      Path documentDirectory, InvertedIndexManager indexManager) {
    return new IcsSearchEngine(documentDirectory, indexManager);
  }

  /** Writes all ICS web page documents in the document directory to the inverted index. */
  public void writeIndex() {
    try {
      Files.walk(Paths.get(documentDirectory.toString(), "cleaned"))
          .filter(Files::isRegularFile)
          .map(Path::toFile)
          .sorted(Comparator.comparingInt(a -> Integer.valueOf(a.getName())))
          .forEach(
              n -> {
                try {

                  String text = new String(Files.readAllBytes(n.toPath()), StandardCharsets.UTF_8);
                  this.indexManager.addDocument(new Document(text));
                } catch (Exception e) {
                  System.out.println(e.toString());
                }
              });

    } catch (Exception e) {
      System.out.println(e.toString());
    }
    this.indexManager.flush();
  }

  /**
   * Computes the page rank score from the "id-graph.tsv" file in the document directory. The
   * results of the computation can be saved in a class variable and will be later retrieved by
   * `getPageRankScores`.
   */
  public void computePageRank(int numIterations) {
    try {
      int size =
          Files.walk(Paths.get(documentDirectory.toString(), "cleaned"))
              .filter(Files::isRegularFile)
              .mapToInt(n -> 1)
              .sum();
      ArrayList<ArrayList<Integer>> nnz = new ArrayList<>(size);
      for (int i = 0; i < size; i++) {
        nnz.add(new ArrayList<>());
      }
      List<String> lines =
          Files.readAllLines(Paths.get(documentDirectory.toString(), "id-graph.tsv"));
      int[] linkCount = new int[size];
      for (String line : lines) {
        String[] l = line.split("\\s");
        int src = Integer.valueOf(l[0]);
        linkCount[src]++;
        int des = Integer.valueOf(l[1]);
        nnz.get(des).add(src);
      }
      nnz.forEach(n -> n.sort(Comparator.naturalOrder()));
      CompRowMatrix matrix =
          new CompRowMatrix(
              size,
              size,
              nnz.stream().map(u -> u.stream().mapToInt(i -> i).toArray()).toArray(int[][]::new));
      //      matrix.transpose();
      double[] data;
      data = matrix.getData();
      for (int i = 0; i < linkCount.length; i++) {
        double v;
        if(linkCount[i]==0) v=0.0;
        else v = 1.0 / linkCount[i];
        int[] colptr = matrix.getColumnIndices();
        for (int j = 0; j < colptr.length; j++) {
          if (colptr[j] == i) {
            data[j] = v;
          }
        }
      }

      DenseVector x = new DenseVector(size);
      DenseVector y = new DenseVector(size);
      double initScore = 1.0 ;/// size;
      double alpha = 0.85;
      double beta = 1 - alpha;
      for (int i = 0; i < x.size(); i++) {
        x.set(i, initScore);
        // y = (1 - alpha) * e / N
        y.set(i, beta);
      }
      for (int epoch = 0; epoch < numIterations; epoch++) {
        // y = alpha*A*x + y
        matrix.multAdd(alpha, x, y);
        double diff = 0;
        for (int i = 0; i < x.size(); i++) {
          diff += Math.abs(x.get(i) - y.get(i));
        }
        DenseVector tmp = y;
        y = x;
        x = tmp;
        for (int i = 0; i < y.size(); i++) {
          y.set(i, beta);
        }
        if (diff < 0.00001) {
          break;
        }
      }
      int index = 0;
      double max = 0;
      for (int i = 0; i < x.size(); i++) {
        if (x.get(i) > max) {
          max = x.get(i);
          index = i;
        }
      }
      System.out.println("MAX SCORE: " + max + " Index: " + index);
      this.pageRankStore = x.getData();
    } catch (Exception e) {
      System.out.println(e.toString());
      //      System.out.println(Arrays.toString(e.getStackTrace()));
      throw new RuntimeException(e);
    }
  }

  /**
   * Gets the page rank score of all documents previously computed. Must be called after
   * `cmoputePageRank`. Returns an list of <DocumentID - Score> Pairs that is sorted by score in
   * descending order (high scores first).
   */
  public List<Pair<Integer, Double>> getPageRankScores() {
    Preconditions.checkNotNull(pageRankStore);
    List<Pair<Integer, Double>> res = new ArrayList<>();
    for (int i = 0; i < pageRankStore.length; i++) {
      res.add(new Pair<>(i, pageRankStore[i]));
    }
    Collections.sort(res, (a, b) -> b.getRight().compareTo(a.getRight()));
//    for (Pair p : res) {
//      System.out.println("Index: " + p.getLeft() + " score: " + p.getRight());
//    }
    return res;
  }

  /**
   * Searches the ICS document corpus and returns the top K documents ranked by combining TF-IDF and
   * PageRank.
   *
   * <p>The search process should first retrieve ALL the top documents from the InvertedIndex by
   * TF-IDF rank, by calling `searchTfIdf(query, null)`.
   *
   * <p>Then the corresponding PageRank score of each document should be retrieved.
   * (`computePageRank` will be called beforehand) For each document, the combined score is
   * tfIdfScore + pageRankWeight * pageRankScore.
   *
   * <p>Finally, the top K documents of the combined score are returned. Each element is a pair of
   * <Document, combinedScore>
   *
   * <p>Note: We could get the Document ID by reading the first line of the document. This is a
   * workaround because our project doesn't support multiple fields. We cannot keep the documentID
   * in a separate column.
   */
  public Iterator<Pair<Document, Double>> searchQuery(
      List<String> query, int topK, double pageRankWeight) {
      Iterator<Pair<Document, Double>> res=indexManager.searchTfIdf(query,null);
      List<Pair<Document, Double>> final_result = new LinkedList<>();
      while(res.hasNext()){
        Pair<Document, Double> result = res.next();
        Document document=result.getLeft();
        String ID=document.getText().split("\n")[0];
        int id=Integer.parseInt(ID);
        double score_tf=result.getRight();
        double new_score=score_tf+pageRankWeight*(double)pageRankStore[id];
        Pair<Document, Double> temp = new Pair<>(document,new_score);
        final_result.add(temp);
      }
      Collections.sort(final_result, new Comparator<Pair<Document, Double>>() {
        @Override
        public int compare(Pair<Document, Double> o1, Pair<Document, Double> o2) {
            Double s1=o1.getRight();
            Double s2=o2.getRight();
            return s2.compareTo(s1);
        }
      });
      //(b,a)->a.getRight().compareTo(b.getRight())
      int size=final_result.size();
      if(topK>size) topK=size;
      List<Pair<Document, Double>> output = new LinkedList<>();
      for(int i=0;i<topK;i++)
        output.add(final_result.get(i));

      return  output.iterator();
  }
}
