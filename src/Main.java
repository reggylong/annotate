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

public class Main {

  private static final String input = "release/crawl";
  public static final String workerInput = "inputs";
  public static final String workerOutput = "outputs";
  private static final String output = "annotations.xml";
  private static final int nWorkers = 16;

  public static void main(String[] args) throws IOException {

    if (!isCached()) {
      System.out.println("Determining size of dataset");
      int count = countLines(input);
      int remaining = count;
      System.out.println("Distributing inputs");
      distributeInputs(count);
    } else {
      System.out.println("Using cached inputs...");
    }
    System.out.println("Processing annotations...");
    processAnnotations();
    System.out.println("Finished processing annotations");
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

  private static boolean isCached() {
    File dir = new File(workerInput);
    boolean cached = true;
    if (!dir.exists()) return false;
    for (int i = 0; i < nWorkers; i++) {
      System.out.println("Checking " + workerInput + "/" + i + ".in");
      File f = new File(workerInput + "/" + i + ".in");
      if (!f.exists()) {
        cached = false;
        break;
      }
    }
    if (new File(workerInput).listFiles().length != nWorkers) {
      cached = false;
    }
    if (!cached) {
      for (File file : dir.listFiles()) file.delete();
    }
    return cached;
  }

  private static void distributeInputs(int total) {
    int remaining = total;
    BufferedReader in = null;
    try {
      in = new BufferedReader(new FileReader(input));
    } catch (IOException e) {
      exit(e);
    }
    File dir = new File("inputs");
    dir.mkdir();
    for (int i = 0; i < nWorkers; i++) {
      PrintWriter w = null;
      try {
        w = new PrintWriter("inputs/" + i + ".in");
      } catch (Exception e) { exit(e); }

      for (int j = 0; j < total / nWorkers; j++) {
        try {
          w.println(in.readLine());
        } catch (IOException e) { printError(e); }
      }
      String line = null;
      if (i == nWorkers - 1) {
        while (true) {
          try {
            line = in.readLine();
          } catch (IOException e) { printError(e); }
          if (line == null) break;
          w.println(line);
        }
      }
      w.close();
    }
    try {
      in.close();
    } catch (IOException e) { printError(e); }
  }

  public static Integer countLines(String filename) {
    Process p = null;
    try {
      p = Runtime.getRuntime().exec("wc -l " + input);
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
    // why java
    return null;
  }

  /*
  String date = object.getJsonString("date").getString();
  String title = object.getJsonString("title").getString();
  String url = object.getJsonString("url").getString();
  String text = object.getJsonString("text").getString();
  int id = object.getInt("articleId");*/

  private static void processAnnotations() {
    File dir = new File(workerOutput);
    dir.mkdir();
    
    for (File file : dir.listFiles()) file.delete();

    Thread threads[] = new Thread[nWorkers];
    for (int i = 0; i < nWorkers; i++) {
      Handler h = new Handler(i);
      threads[i] = new Thread(h);
      threads[i].start();
    }
    for (int i = 0; i < nWorkers; i++) {
      try {
      threads[i].join();
      } catch (InterruptedException e) { Main.printError(e); }
    }
  }
}

class Handler implements Runnable {
  private final int id;
  private PrintWriter xmlOut;
  private StanfordCoreNLP pipeline;
  private BufferedReader in;
  private int remainingLines;

  Handler(int id) { 
    this.id = id;
    pipeline = initPipeline();
    initOut();
    initIn();
  }

  private static StanfordCoreNLP initPipeline() {
    Properties props = new Properties();
    props.setProperty("annotators", "tokenize, ssplit, pos, lemma, ner");
    return new StanfordCoreNLP(props);
  }

  private void initOut() {
    String output = id + ".xml";
    try {
      xmlOut = new PrintWriter(Main.workerOutput + "/" + output);
    } catch (IOException e) {
      System.err.println(e.getMessage());
      System.exit(1);
    }
  }

  private void initIn() {
    String input = id + ".in";
    try {
      in = new BufferedReader(new FileReader(Main.workerInput + "/" + input));
    } catch (IOException e) {
      Main.exit(e);
    }
    remainingLines = Main.countLines(Main.workerInput + "/" + input);
  }

  private JsonObject read() {
    String line = null;
    try {
      line = in.readLine();
    } catch (IOException e) {
      Main.printError(e);
      return null;
    }
    if (line == null) return null;
    JsonReader reader = Json.createReader(new StringReader(line));
    JsonObject object = reader.readObject();
    reader.close();
    return object;
  }

  public void run() { 
    JsonObject obj = null;
    while ( (obj = read()) != null) {

      Annotation annotation = null;
      try {
        annotation = new Annotation(obj.getJsonString("text").getString());
      } catch (Exception e) {
        Main.printError(e);
        System.err.println(obj);
      }
      pipeline.annotate(annotation);
      try {
        xmlOut.println(obj.getInt("articleId"));
        pipeline.xmlPrint(annotation, xmlOut);  
        xmlOut.println();
      } catch (Exception e) {
        Main.printError(e);
      }
      remainingLines--;
      if (remainingLines % 500 == 0) {
        synchronized (System.out) {
          System.out.println("Worker " + id + ": " 
              + remainingLines + " examples left");
        }
      }
    }
    IOUtils.closeIgnoringExceptions(xmlOut);
  }
}
