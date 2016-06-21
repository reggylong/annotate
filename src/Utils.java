import java.io.*;
import java.util.*;

import edu.stanford.nlp.hcoref.data.CorefChain;
import edu.stanford.nlp.hcoref.CorefCoreAnnotations;
import edu.stanford.nlp.io.*;
import edu.stanford.nlp.ling.*;
import edu.stanford.nlp.pipeline.*;
import edu.stanford.nlp.semgraph.SemanticGraph;
import edu.stanford.nlp.semgraph.SemanticGraphCoreAnnotations;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.trees.*;
import edu.stanford.nlp.util.*;
import javax.json.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.*;
import java.lang.*;

public class Utils {

  public static StanfordCoreNLP initPipeline() {
    Properties props = new Properties();
    props.setProperty("annotators", "tokenize, ssplit, pos, lemma, ner, parse, dcoref");
    return new StanfordCoreNLP(props);
  }

  public static void printError(Exception e) {
    System.err.println(e.getMessage());
    e.printStackTrace();
  }

  public static void exit(Exception e) {
    printError(e);
    System.err.println("Exiting...");
    System.exit(1);
  }

  public static String key(int group, int id) {
    return group + "-" + id;
  }

  public static BufferedReader initIn(int group, int id) {
    String input = key(group, id) + ".in";
    try {
      return new BufferedReader(new FileReader(Main.workerInput + "/" + input));
    } catch (IOException e) {
      exit(e);
    }
    return null;
  }

  private static PrintWriter initOut(int group, int id) {
    String output = key(group, id) + ".out";
    try {
      return new PrintWriter(Main.workerOutput + "/" + output);
    } catch (IOException e) {
      System.err.println(e.getMessage());
      System.exit(1);
    }
    return null;
  }

  public static boolean isCached(int group) {
    File dir = new File(Main.workerInput);
    boolean cached = true;
    if (!dir.exists()) return false;
    for (int i = 0; i < Main.nWorkers; i++) {
      System.out.println("Checking " + Main.workerInput + "/" + key(group, i) + ".in");
      File f = new File(Main.workerInput + "/" + key(group,i) + ".in");
      if (!f.exists()) {
        cached = false;
        break;
      }
    }
    return cached;
  }

  public static Integer countLines(String filename) {
    Process p = null;
    try {
      p = Runtime.getRuntime().exec("wc -l " + filename);
      p.waitFor();
      BufferedReader reader = new BufferedReader(
          new InputStreamReader(p.getInputStream()));
      String line = reader.readLine();
      return Integer.parseInt(line.split("\\s+")[0]);
    } catch (IOException e) {
      exit(e);
    } catch (InterruptedException e) {
      exit(e);
    }
    return null;
  }

}
