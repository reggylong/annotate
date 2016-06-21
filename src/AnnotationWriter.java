import java.io.*;
import java.util.*;

import java.util.concurrent.*;
import java.util.concurrent.locks.*;
import java.lang.*;

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

class AnnotationWriter implements Runnable {
 
  private final StanfordCoreNLP pipeline;
  private final BlockingQueue<Annotation> annotations;
  private final BlockingQueue<PrintWriter> writers;
  private final int nWriters;

  AnnotationWriter(BlockingQueue<Annotation> annotations, BlockingQueue<PrintWriter> writers, int nWriters) {
    this.annotations = annotations; 
    this.pipeline = Utils.initPipeline();
    this.writers = writers;
    this.nWriters = nWriters;
  }

  public void run() {
    ExecutorService pool = Executors.newFixedThreadPool(nWriters);
    while (true) {
      try {
        Annotation annotation = annotations.take();
        pool.submit(new SingleAnnotationHandler(pipeline, annotation, writers));
      } catch (InterruptedException e) {
        Utils.printError(e);
        pool.shutdown();
        try {
          pool.awaitTermination(Integer.MAX_VALUE, TimeUnit.SECONDS);
        } catch (InterruptedException ex) {}
      }
        
    }
  }
}

class SingleAnnotationHandler implements Runnable {

  private final StanfordCoreNLP pipeline;
  private final Annotation annotation;
  private final BlockingQueue<PrintWriter> writers;

  SingleAnnotationHandler(StanfordCoreNLP pipeline, Annotation annotation, BlockingQueue<PrintWriter> writers) {
    this.pipeline = pipeline;
    this.annotation = annotation;
    this.writers = writers;
  }

  public void run() {
    PrintWriter w = null;
    try {
      w = writers.take();
      pipeline.jsonPrint(annotation, w);
    } catch (InterruptedException e) { 
      Utils.printError(e); 
    } catch (IOException e) {
      Utils.printError(e);
    }
    if (w == null) return;
    try {
      writers.put(w);
    } catch (InterruptedException e) { 
      Utils.printError(e);
      writers.offer(w); 
    }
  }
}
