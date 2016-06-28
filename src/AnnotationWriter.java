import java.io.*;
import java.util.*;

import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
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
  private final BlockingQueue<Pair<String, Annotation>> annotations;
  private final PrintWriter writer;
  private long count = 0;
  

  AnnotationWriter(BlockingQueue<Pair<String,Annotation>> annotations, PrintWriter writer) {
    this.annotations = annotations; 
    this.pipeline = Utils.initPipeline();
    this.writer = writer;
  }

  public void run() {
    while (true) {
      Pair<String,Annotation> annotation = null;
      try {
        annotation = annotations.take();
      } catch (InterruptedException e) {
        Utils.printError(e);
        continue;
      }
      if (annotation == Main.POISON_PILL) return;
      try {
        writer.println(annotation.first);
        pipeline.jsonPrint(annotation.second, writer);
      } catch (IOException e) {
        Utils.printError(e);
      }
      writer.println();
      count++;
      if (count % 10 == 0) {
        System.out.println(Utils.getElapsed() + count + " number of written examples");
      }
        
    }
  }
}

