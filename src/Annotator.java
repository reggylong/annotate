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
  private AtomicInteger count;
  private Annotation annotation;
  private BlockingQueue<Annotation> annotations;

  Annotator(StanfordCoreNLP pipeline, BlockingQueue<Annotation> annotations, Annotation annotation, AtomicInteger count) {
    this.count = count;
    this.annotation = annotation;
    // Used to feed input to AnnotationWriter
    this.annotations = annotations;
    this.pipeline = pipeline;
  }
  
  public void run() {
    try {
      pipeline.annotate(annotation);
      annotations.put(annotation);
    } catch (Exception e) {
      Utils.printError(e);
    }
    count.getAndIncrement();
  }
}
