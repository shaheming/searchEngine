package edu.uci.ics.cs221.index.positional;

import edu.uci.ics.cs221.analysis.Analyzer;
import edu.uci.ics.cs221.analysis.ComposableAnalyzer;
import edu.uci.ics.cs221.analysis.PorterStemmer;
import edu.uci.ics.cs221.analysis.PunctuationTokenizer;
import edu.uci.ics.cs221.index.inverted.DeltaVarLenCompressor;
import edu.uci.ics.cs221.index.inverted.InvertedIndexManager;
import edu.uci.ics.cs221.index.inverted.NaiveCompressor;
import edu.uci.ics.cs221.index.inverted.PageFileChannel;
import edu.uci.ics.cs221.storage.Document;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Team5Test {
  private DeltaVarLenCompressor compressor = new DeltaVarLenCompressor();
  private NaiveCompressor naivecompressor = new NaiveCompressor();
  private String path = "./index/ranking";
  private Analyzer analyzer =
      new ComposableAnalyzer(new PunctuationTokenizer(), new PorterStemmer());
  private InvertedIndexManager invertList;

  @Before
  public void setup() {
    File directory1 = new File(path);
    if (!directory1.exists()) {
      directory1.mkdirs();
    }

    invertList = InvertedIndexManager.createOrOpenPositional(path, analyzer, compressor);
  }

  // test simple documents with same text, each key word show only one time each document
  // mainly test inverted list since inverted list is long but positional list is short
  @Test
  public void Test1() {
    Assert.assertEquals(0, PageFileChannel.readCounter);
    Assert.assertEquals(0, PageFileChannel.writeCounter);
    invertList.addDocument(new Document("new york city"));
    invertList.addDocument(new Document("los angeles city"));
    invertList.flush();
    invertList.addDocument(new Document("new york post"));
    invertList.flush();
    final PorterStemmer p = new PorterStemmer();
    List<String> keywords =
        Stream.of("new", "new", "city").map(p::stem).collect(Collectors.toList());

    Iterator<Document> documentIterator = invertList.searchTfIdf(keywords, 3);

    while (documentIterator.hasNext()) {
      System.out.println(documentIterator.next().getText());
    }
  }

  @After
  public void cleanup() throws Exception {
    PageFileChannel.resetCounters();
    Path rootPath = Paths.get("./index/ranking");
    Files.walk(rootPath).sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
    Files.deleteIfExists(rootPath);
  }
}
