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

public class Main {

  private static StanfordCoreNLP pipeline;
  private static final String input = "release/crawl";
  private static BufferedReader in;

  public static void main(String[] args) throws IOException {

    initPipeline();
    openInput(input);
    System.out.println("Finished loading input:" + input);
    System.out.println("Processing annotations...");
    processAnnotations();
    System.out.println("Finished processing annotations");
  }

  private static void initPipeline() {
    Properties props = new Properties();
    props.setProperty("annotators", "tokenize, ssplit, pos, lemma, ner, parse, dcoref");
    pipeline = new StanfordCoreNLP(props);
  }

  private static void openInput(String filename) {
    try {
      in = new BufferedReader(new FileReader(filename));
    } catch (FileNotFoundException e) {
      System.err.println(e.getMessage());
      System.exit(1);
    }
  }

  private static JsonObject read() {
    String line = null;
    try {
      line = in.readLine();
    } catch (IOException e) {
      System.err.println(e.getMessage());
      return null;
    }
    if (line == null) return null;
    JsonReader reader = Json.createReader(new StringReader(line));
    JsonObject object = reader.readObject();
    reader.close();
    return object;
  }

  /*
  String date = object.getJsonString("date").getString();
  String title = object.getJsonString("title").getString();
  String url = object.getJsonString("url").getString();
  String text = object.getJsonString("text").getString();
  int id = object.getInt("articleId");*/

  private static void processAnnotations() {
    JsonObject obj = null;
    PrintWriter xmlOut = null;
    try {
      xmlOut = new PrintWriter("output.xml");
    } catch (Exception e) {
      System.err.println(e.getMessage());
      System.exit(1);
    }
    while ( (obj = read()) != null) {
      Annotation annotation = null;
      try {
        annotation = new Annotation(obj.getJsonString("text").getString());
      } catch (Exception e) {
        System.err.println(e.getMessage());
        System.err.println(obj);
        continue;
      }
      pipeline.annotate(annotation);
      try {
        pipeline.xmlPrint(annotation, xmlOut);  
      } catch (Exception e) {}
      xmlOut.println();
    }
    IOUtils.closeIgnoringExceptions(xmlOut);
  }
}
