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
import java.util.concurrent.atomic.*;
import java.lang.*;

public class Main {

  public static final String dataset = "release/crawl";
  public static final String inputPath = "inputs";
  public static final String outputPath = "outputs";
  public static final String failedPath = "failed";
  public static int nGroups = 10;
  public static final int nWorkers = 24;
  public static int group = 0;
  public static final AtomicInteger count = new AtomicInteger(0);
  public static final AtomicInteger failed = new AtomicInteger(0);
  public static final AtomicInteger malformed = new AtomicInteger(0);
  public static int timeout = 60;
  public static long startTime; 


  public static void main(String[] args) throws IOException {
    startTime = System.nanoTime();
    if (args.length > 0) {
      group = Integer.parseInt(args[0]);
    }
    redirect(); 

    System.out.println("Determining size of dataset");
    int lines = Utils.countLines(dataset);

    if (!Utils.isCached(inputPath, nGroups)) {
      System.out.println("Distributing inputs");
      Utils.distributeInputs(dataset, inputPath, lines, nGroups);
    } else {
      System.out.println("Using cached inputs...");
    }
    System.out.println("Processing annotations...");
    processAnnotations(group);
    System.out.println("Finished processing annotations");
  }

  private static void redirect() {
    String logPath = "logs";
    File dir = new File(logPath);
    dir.mkdir();

    try {
      PrintStream out = new PrintStream(new FileOutputStream(logPath + "/" + group + ".out"));
      PrintStream err = new PrintStream(new FileOutputStream(logPath + "/" + group + ".err"));
      System.setOut(out);
      System.setErr(err);
    } catch (FileNotFoundException e) {
      Utils.exit(e);
    }
  }

  /*
  String date = object.getJsonString("date").getString();
  String title = object.getJsonString("title").getString();
  String url = object.getJsonString("url").getString();
  String text = object.getJsonString("text").getString();
  int id = object.getInt("articleId");*/

  private static void processAnnotations(int group) {
    File dir = new File(outputPath);
    dir.mkdir();

    File failedDir = new File(failedPath);
    failedDir.mkdir();

    ExecutorService scheduler = Executors.newFixedThreadPool(nWorkers);

    ExecutorService pool = Executors.newFixedThreadPool(nWorkers);

    PrintWriter w = new PrintWriter(Utils.initOut(outputPath, group));
    PrintWriter failedW = new PrintWriter(Utils.initOut(failedPath, group));
    BlockingQueue<Pair<String, Annotation>> annotations = new LinkedBlockingQueue<>();
    Thread writer = new Thread(new AnnotationWriter(annotations, w));
    writer.start();

    BufferedReader in = Utils.initIn(inputPath, group);
    StanfordCoreNLP pipeline = Utils.initPipeline(); 

    JsonObject obj = null;
    while ( (obj = Utils.read(in)) != null) {
      try {
        String id = obj.getInt("articleId") + ""; 
        Runnable runner = new Annotator(pipeline, annotations, obj);
        scheduler.submit(new TimeoutRunner(pool, id, failedW, runner, timeout));
      } catch (Exception e) { Utils.printError(e); }
    }

    scheduler.shutdown();
    try {
      scheduler.awaitTermination(7, TimeUnit.DAYS); 
    } catch (InterruptedException e) {}

    pool.shutdown(); 
    try {
      scheduler.awaitTermination(7, TimeUnit.DAYS); 
    } catch (InterruptedException e) {}

    writer.interrupt();
    try {
      writer.join();
    } catch (InterruptedException e) { 
      Utils.printError(e);
    }
    IOUtils.closeIgnoringExceptions(w);
    IOUtils.closeIgnoringExceptions(failedW);
  }
}
