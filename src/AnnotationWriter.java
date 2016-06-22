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
  private final BlockingQueue<AnnotationWrapper> annotations;
  private final PrintWriter writer;

  AnnotationWriter(BlockingQueue<AnnotationWrapper> annotations, PrintWriter writer) {
    this.annotations = annotations; 
    this.pipeline = Utils.initPipeline();
    this.writer = writer;
  }

  public void run() {
    while (true) {
      AnnotationWrapper annotation = null;
      try {
        annotation = annotations.take();
      } catch (InterruptedException e) {
        Utils.printError(e);
      }
      try {
        writer.println(annotation.date);
        pipeline.xmlPrint(annotation.annotation, writer);
        writer.println();
      } catch (IOException e) {
        Utils.printError(e);
      }
        
    }
  }
}

