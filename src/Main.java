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

  private static List<Annotation> annotations = new ArrayList<>();
  private static StanfordCoreNLP pipeline;
  private static final String input = "release/crawl";

  public static void main(String[] args) throws IOException {

    //initPipeline();
    loadUtterances(input);
    System.out.println("Finished loading input:" + input);
    System.out.println("Processing annotations...");
    processAnnotations();
  }

  private static void initPipeline() {
    Properties props = new Properties();
    props.setProperty("annotators", "tokenize, ssplit, pos, lemma, ner, parse, dcoref");
    pipeline = new StanfordCoreNLP(props);
  }

  private static void loadUtterances(String filename) {

    BufferedReader in = null;
    try {
      in = new BufferedReader(new FileReader(filename));
    } catch (FileNotFoundException e) {
      System.err.println(e.getMessage());
      System.exit(1);
    }

    for (int i = 0; i < 10; i++) {
      String line = null;
      try {
        line = in.readLine();
      } catch (IOException e) { 
        System.err.println(e.getMessage());
        continue;
      }
      JsonReader reader = Json.createReader(new StringReader(line));
      JsonObject object = reader.readObject();

      String date = object.getJsonString("date").getString();
      String title = object.getJsonString("title").getString();
      String url = object.getJsonString("url").getString();
      String text = object.getJsonString("text").getString();
      int id = object.getInt("articleId");
      System.out.println(id);
      reader.close();
    }
    System.exit(0);
  }

  private static void processAnnotations() {
    PrintWriter xmlOut = null;
    try {
      xmlOut = new PrintWriter("output");
    } catch (Exception e) {}
    // Initialize an Annotation with some text to be annotated. The text is the argument to the constructor.
    Annotation annotation;
    annotation = new Annotation("Kosgi Santosh sent an email to Stanford University. He didn't get a reply.");

    // run all the selected Annotators on this text
    pipeline.annotate(annotation);

    try {
      pipeline.xmlPrint(annotation, xmlOut);
    } catch (Exception e) {}
    IOUtils.closeIgnoringExceptions(xmlOut);

  }
}
