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

  public static final String input = "release/crawl";
  public static final String workerInput = "inputs";
  public static final String workerOutput = "outputs";
  public static final int nWorkers = 24;
  public static final int timeout = 60;

  public static void main(String[] args) throws IOException {

    if (!isCached()) {
      System.out.println("Determining size of dataset");
      int count = Utils.countLines(input);
      System.out.println("Distributing inputs");
      distributeInputs(count);
    } else {
      System.out.println("Using cached inputs...");
    }
    System.out.println("Processing annotations...");
    processAnnotations();
    System.out.println("Finished processing annotations");
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
    BufferedReader in = null;
    try {
      in = new BufferedReader(new FileReader(input));
    } catch (IOException e) {
      Utils.exit(e);
    }
    File dir = new File("inputs");
    dir.mkdir();
    for (int i = 0; i < nWorkers; i++) {
      PrintWriter w = null;
      try {
        w = new PrintWriter("inputs/" + i + ".in");
      } catch (Exception e) { Utils.exit(e); }

      for (int j = 0; j < total / nWorkers; j++) {
        try {
          w.println(in.readLine());
        } catch (IOException e) { Utils.printError(e); }
      }
      String line = null;
      if (i == nWorkers - 1) {
        while (true) {
          try {
            line = in.readLine();
          } catch (IOException e) { Utils.printError(e); }
          if (line == null) break;
          w.println(line);
        }
      }
      w.close();
    }
    try {
      in.close();
    } catch (IOException e) { Utils.printError(e); }
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
      } catch (InterruptedException e) { Utils.printError(e); }
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
    props.setProperty("annotators", "tokenize, ssplit, pos, lemma, ner, parse, dcoref");
    return new StanfordCoreNLP(props);
  }

  private void initOut() {
    String output = id + ".out";
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
      Utils.exit(e);
    }
    remainingLines = Utils.countLines(Main.workerInput + "/" + input);
  }

  private JsonObject read() {
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

  public void run() { 
    ExecutorService executor = Executors.newFixedThreadPool(1);
    JsonObject obj = null;
    long startTime = System.nanoTime();
    while ( (obj = read()) != null) {
      remainingLines--;
      if (remainingLines % 100 == 0) {
        long diffTime = System.nanoTime() - startTime;
        synchronized (System.out) {
          System.out.println("[" + TimeUnit.NANOSECONDS.toMinutes(diffTime) + 
              " mins elapsed]" + "Worker " + id + ": " 
              + remainingLines + " examples left");
        }
      }

      Annotation annotation = null;
      if (obj.getJsonString("text") == null) continue;
      annotation = new Annotation(obj.getJsonString("text").getString());

      Future<Boolean> future = executor.submit(new Annotator(pipeline, annotation));
      try {
        boolean success = future.get(Main.timeout, TimeUnit.SECONDS);
        if (!success) continue;
      } catch (Exception e) {
        if (!future.isCancelled() && !future.isDone()) {
          future.cancel(true);
        }
        continue;
      }
      try {
        xmlOut.println(obj.getInt("articleId"));
        pipeline.xmlPrint(annotation, xmlOut);  
        xmlOut.println();
      } catch (Exception e) {
        Utils.printError(e);
      }
      
    }
    IOUtils.closeIgnoringExceptions(xmlOut);
  }
}

class Annotator implements Callable<Boolean> {

  private StanfordCoreNLP pipeline;
  private Annotation annotation;

  Annotator(StanfordCoreNLP pipeline, Annotation annotation) {
    this.pipeline = pipeline;
    this.annotation = annotation;
  }

  public Boolean call() {
    try {
      pipeline.annotate(annotation);
    } catch (Exception e) {
      Utils.printError(e);
      return false;
    }
    return true;
  }
}
