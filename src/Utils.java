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
    props.setProperty("annotators", "tokenize, ssplit, pos, lemma, ner, parse, dcoref ");
    props.setProperty("dcoref.maxdist", "3");
    props.setProperty("parse.model", "resources/edu/stanford/nlp/models/srparser/englishSR.ser.gz");
    props.setProperty("openie.max_entailments_per_clause", "100");
    return new StanfordCoreNLP(props);
  }

  public static StanfordCoreNLP initMiniPipeline() {
    Properties props = new Properties();
    props.setProperty("annotators", "tokenize, ssplit, pos, lemma, ner");
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

  public static BufferedReader initIn(String inputPath, int group) {
    String name = group + ".in";
    try {
      return new BufferedReader(new FileReader(inputPath + "/" + name));
    } catch (IOException e) {
      exit(e);
    }
    return null;
  }

  public static PrintWriter initOut(String outputPath, int group) {
    String output = group + ".out";
    try {
      return new PrintWriter(outputPath + "/" + output);
    } catch (IOException e) {
      System.err.println(e.getMessage());
      System.exit(1);
    }
    return null;
  }

  public static boolean isCached(String inputPath, int nGroups) {
    File dir = new File(inputPath);
    boolean cached = true;
    if (!dir.exists()) return false;
    for (int i = 0; i < nGroups; i++) {
      System.out.println("Checking " + inputPath + "/" + i + ".in");
      File f = new File(inputPath + "/" + i + ".in");
      if (!f.exists()) {
        System.out.println("Missing file " + inputPath + "/" + i + ".in");
        cached = false;
        break;
      }
    }
    if (new File(inputPath).list().length != nGroups) cached = false;
    if (!cached) for (File file : dir.listFiles()) file.delete();

    return cached;
  }

  public static void distributeInputs(String datasetPath, String workerInput, int total, int nGroups) {
    BufferedReader in = null;
    try {
      in = new BufferedReader(new FileReader(datasetPath));
    } catch (IOException e) {
      Utils.exit(e);
    }
    File dir = new File(workerInput);
    dir.mkdir();

    List<PrintWriter> inputWriters = new ArrayList<>();
    for (int i = 0; i < nGroups; i++) {
      PrintWriter w = null;
      try {
        inputWriters.add(new PrintWriter("inputs/" + i + ".in"));
      } catch (Exception e) { Utils.exit(e); }
    }

    long i = 0;
    String line = null;
    while (true) {
      try {
        line = in.readLine();
      } catch (IOException e) { Utils.printError(e); }
      if (line == null) break;
      inputWriters.get((int) i % (inputWriters.size())).println(line);
      i++;
    }
    for (int j = 0; j < nGroups; i++) {
      inputWriters.get(j).close();
    }

    try {
      in.close();
    } catch (IOException e) { Utils.printError(e); }
  } 

  public static JsonObject read(BufferedReader in) {
    String line = null;
    try {
      line = in.readLine();
    } catch (IOException e) {
      Utils.printError(e);
      return null;
    }
    if (line == null) return null;
    JsonReader reader = Json.createReader(new StringReader(line));
    JsonObject object = reader.readObject();
    reader.close();
    return object;
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

  public static String hostname() {
    Process p = null;
    try {
      p = Runtime.getRuntime().exec("hostname");
      p.waitFor();
      BufferedReader reader = new BufferedReader(
          new InputStreamReader(p.getInputStream()));
      String line = reader.readLine();
      return line.split("\\.")[0];
    } catch (IOException e) {
      exit(e);
    } catch (InterruptedException e) {
      exit(e);
    }
    return null;

  }

  public static String getElapsed() {
    return "[" + TimeUnit.NANOSECONDS.toMinutes(System.nanoTime() - Main.startTime) + " min(s) elapsed]";
  }
  public static void printFailed(int failed) {
    if (failed % 10 == 0) {
      System.out.println(getElapsed() + failed + " number of executions have failed.");
    }
  }

}
