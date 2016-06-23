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

class Annotator implements Runnable {
  private StanfordCoreNLP pipeline;
  private JsonObject obj;
  private BlockingQueue<Annotation> annotations;

  Annotator(StanfordCoreNLP pipeline, BlockingQueue<Annotation> annotations, JsonObject obj) {
    this.obj = obj;
    // Used to feed input to AnnotationWriter
    this.annotations = annotations;
    this.pipeline = pipeline;
  }

  public void run() {
    Annotation annotation = null;
    if (obj.getJsonString("text") != null && obj.getJsonString("date") != null) {
      annotation = new Annotation(obj.getString("text"));
      try {
        pipeline.annotate(annotation);
        annotations.put(annotation);
      } catch (Exception e) {
        Utils.printError(e);
      }
    } else {

    }
    int examined = Main.count.incrementAndGet();
    if (examined % 10 == 0) {
      long diffTime = System.nanoTime() - Main.startTime;
      System.out.println("[" + TimeUnit.NANOSECONDS.toMinutes(diffTime) +
          " min(s) elapsed]" +
          + examined + " examined");
    }
  }
}
