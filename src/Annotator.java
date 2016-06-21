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

class Annotator implements Runnable {
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
