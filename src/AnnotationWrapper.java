

import edu.stanford.nlp.pipeline.*;

class AnnotationWrapper {
  public static Annotation annotation;
  public static String title;
  public static String date;
  public static String id;
  public static String url;

  AnnotationWrapper(Annotation annotation, String date) {
    this.annotation = annotation;
    this.date = date;
  }

}
