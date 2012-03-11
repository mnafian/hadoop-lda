package org.thunlp.learning.lda;


public class Similarity {
  public static double KLDivergence(double [] pa, double [] pb) {
    return 0.0;
  }
  
  public static double JSDivergence(double [] pa, double [] pb) {
    return 0.0;
  }
  
  public static double EuclideanDistance(double [] pa, double [] pb) {
    double d = 0.0;
    if (pa.length != pb.length) {
      throw new RuntimeException("pa and pb have different length.");
    }
    for (int i = 0; i < pa.length; i++) {
      d += (pa[i] - pb[i]) * (pa[i] - pb[i]);
    }
    return Math.sqrt(d);
  }
  
  public static double CosineSimilarity(double [] pa, double [] pb) {
    double d = 0.0;
    if (pa.length != pb.length) {
      throw new RuntimeException("pa and pb have different length.");
    }
    for (int i = 0; i < pa.length; i++) {
      d += pa[i] * pb[i];
    }
    return d;
  }
}
