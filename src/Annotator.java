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
  private BlockingQueue<Pair<String,Annotation>> annotations;

  Annotator(StanfordCoreNLP pipeline, BlockingQueue<Pair<String,Annotation>> annotations, JsonObject obj) {
    this.obj = obj;
    // Used to feed input to AnnotationWriter
    this.annotations = annotations;
    this.pipeline = pipeline;
  }

  public void run() {
    Annotation annotation = null;
    if (obj.getJsonString("text") != null && obj.getJsonString("date") != null) {
      annotation = new Annotation(obj.getString("text"));
      pipeline.annotate(annotation);
      try {
        annotations.put(new Pair<>(obj.getInt("articleId") + " " + obj.getString("date"),annotation));
      } catch (InterruptedException e) {
        int failed = Main.failed.incrementAndGet();
        Utils.printFailed(failed);
      }
    } else {
      int malformed = Main.malformed.incrementAndGet();
      if (malformed % 10 == 0) {
        System.out.println(malformed + " number of malformed examples");
      }
    }
  }
}
