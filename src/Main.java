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
  public static int nGroups = 8;
  public static final int nWorkers = 20;
  public static int group = 0;
  private static final AtomicInteger count = new AtomicInteger(0);
  public static int timeout = 120;


  public static void main(String[] args) throws IOException {
    if (args.length > 0) {
      group = Integer.parseInt(args[0]);
    }
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

  /*
  String date = object.getJsonString("date").getString();
  String title = object.getJsonString("title").getString();
  String url = object.getJsonString("url").getString();
  String text = object.getJsonString("text").getString();
  int id = object.getInt("articleId");*/

  private static void processAnnotations(int group) {
    File dir = new File(outputPath);
    dir.mkdir();

    ExecutorService scheduler = Executors.newFixedThreadPool(nWorkers);

    ExecutorService pool = Executors.newFixedThreadPool(nWorkers);

    PrintWriter w = new PrintWriter(Utils.initOut(outputPath, group));

    BlockingQueue<Annotation> annotations = new LinkedBlockingQueue<Annotation>();
    Thread writer = new Thread(new AnnotationWriter(annotations, w));
    writer.start();

    BufferedReader in = Utils.initIn(inputPath, group);
    StanfordCoreNLP pipeline = Utils.initPipeline(); 

    JsonObject obj = null;
    long startTime = System.nanoTime();
    int prevCompleted = 0;
    int completed = count.get();
    while ( (obj = Utils.read(in)) != null) {
      Annotation annotation = null;
      if (obj.getJsonString("text") == null) continue;
      annotation = new Annotation(obj.getJsonString("text").getString());
      Runnable runner = new Annotator(pipeline, annotations, annotation, count);
      scheduler.submit(new TimeoutRunner(pool, runner, timeout));

      prevCompleted = completed; 
      completed = count.get();
      if (completed % 100 == 0 && completed != prevCompleted) {
        long diffTime = System.nanoTime() - startTime;
        System.out.println("[" + TimeUnit.NANOSECONDS.toMinutes(diffTime) + 
            " min(s) elapsed]" +
            + completed + " completed");
      }
    }

    pool.shutdown(); 
    scheduler.shutdown();
    try {
      pool.awaitTermination(7, TimeUnit.DAYS); 
    } catch (InterruptedException e) {}

    writer.interrupt();
    try {
      writer.join();
    } catch (InterruptedException e) { 
      Utils.printError(e);
    }
    IOUtils.closeIgnoringExceptions(w);
  }
}
