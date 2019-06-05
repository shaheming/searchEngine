package edu.uci.ics.cs221.index.inverted;

import com.google.common.base.Preconditions;
import edu.uci.ics.cs221.storage.Document;
import javafx.util.Pair;
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

  private static void accept(File f) {
    System.out.println(f.getName());
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
                  // System. out. println(text);
                  // Exit(0);
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
   * Computes the page rank score from the id-graph file in the document directory. The results of
   * the computation can be saved in a class variable and will be later retrieved by
   * `getPageRankScores`.
   */
  public void computePageRank() {
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
//      for (int i = 0; i < linkCount.length; i++) linkCount[i] = 0;
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
        double v = 1.0 / linkCount[i];
        int[] colptr = matrix.getColumnIndices();
        for (int j = 0; j < colptr.length; j++) {
          if (colptr[j] == i) {
            data[j] = v;
          }
        }
      }

      DenseVector x = new DenseVector(size);
      DenseVector y = new DenseVector(size);
      double initScore = 1.0 / size;
      double alpha = 0.8;
      double beta = 1 - alpha;
      double inity = beta * initScore;
      for (int i = 0; i < x.size(); i++) {
        x.set(i, initScore);
        // y = (1 - alpha) * e / N
        y.set(i, inity);
      }
      for (int epoch = 0; epoch < 50; epoch++) {
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
          y.set(i, inity);
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
   * `computePageRank`. Returns a list of <DocumentID - Score> Pairs that is sorted by score in
   * descending order (high scores first).
   */
  public List<Pair<Integer, Double>> getPageRankScores() {
    Preconditions.checkNotNull(pageRankStore);
    List<Pair<Integer, Double>> res = new ArrayList<>();
    for (int i = 0; i < pageRankStore.length; i++) {
      res.add(new Pair<>(i, pageRankStore[i]));
    }
    Collections.sort(res, (a, b) -> b.getValue().compareTo(a.getValue()));
    for (Pair p : res) {
      System.out.println("Index: " + p.getKey() + " score: " + p.getValue());
    }
    return res;
  }

  /**
   * Searches the ICS document corpus with the query using TF-IDF search provided by inverted index
   * manager. The search process should first retrieve the top-K documents by TF-IDF rank, then
   * re-order the resulting documents by the page rank score.
   *
   * <p>Note: you could get the ID of each document from its first line.
   */
  public Iterator<Document> searchQuery(List<String> query, int topK) {
    throw new UnsupportedOperationException();
  }
}
