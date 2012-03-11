package org.thunlp.learning.lda;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Random;
import java.util.Map.Entry;
import java.util.logging.Logger;

import org.thunlp.io.GzipTextFileReader;
import org.thunlp.io.TextFileReader;
import org.thunlp.tool.StringUtil;

/**
 * Load a trained LDA model, do various inferences about it.
 * @author sixiance
 *
 */
public class LdaModel {
  protected static Logger LOG = Logger.getLogger(LdaModel.class.getName());
  protected Hashtable<String, int []> nwz = new Hashtable<String, int []>();
  protected int [] topicSum;
  protected int totalSum;
  protected int numTopics;
  protected double alpha;
  protected double beta;
  protected int n;  // Number of iterations.
  protected String [] explanations;
  protected ArrayList<String> words = new ArrayList<String>();
  
  protected int [] ndz;
  protected Random random = new Random();
  
  public static double LOG_MIN_PROB = -7;

  public double [] inference(String [] words) {
    double [] dist = new double[numTopics];
    inference(words, dist, 30, 10);
    return dist;
  }
  
  public String [] removeUnknownWords(String [] words) {
    ArrayList<String> features = new ArrayList<String>();
    for (String word : words) {
      if (nwz.containsKey(word)) {
        features.add(word);
      }
    }
    return features.toArray(new String[0]);
  }
  

  public void inference(String [] words, double [] pz,
      int numBurnInIterations, int numSamplingIterations) {
    words = removeUnknownWords(words);
    int [] z = new int[words.length];
    
    for (int i = 0; i < numTopics; i++) {
      ndz[i] = 0;
    }
    for (int i = 0; i < words.length; i++) {
      z[i] = random.nextInt(numTopics);
      ndz[z[i]]++;
    }
    
    // Burn-in.
    for (int i = 0; i < numBurnInIterations; i++) {
      for (int j = 0; j < words.length; j++) {
        int oldTopic = z[j];
        --ndz[oldTopic];
        calculateConditionalProbability(words[j], ndz, pz, words.length);
        int newTopic = sampleInDistribution(pz);
        z[j] = newTopic;
        ++ndz[newTopic];   
      }
    }
    
    // Inference. 
    for (int i = 0; i < numSamplingIterations; i++) {
      for (int j = 0; j < words.length; j++) {
        calculateConditionalProbability(words[j], ndz, pz, words.length);
        int newTopic = sampleInDistribution(pz);
        z[j] = newTopic;
        ++ndz[newTopic];   
      }
    }
    
    double norm = 0.0;
    for (int i = 0; i < pz.length; i++) {
      pz[i] = ndz[i] + alpha;
      norm += pz[i];
    }
    for (int i = 0; i < pz.length; i++) {
      pz[i] /= norm;
    } 
  }
  

  public double [] inferenceFast(String [] doc) {
    double [] p = new double[numTopics];
    inferenceFast(doc, p);
    return p;
  }
  
  public void inferenceFast(String [] doc, double [] p) {
    for (int i = 0; i < numTopics; i++) {
      p[i] = 0.0;
    }
    for (int i = 0; i < doc.length; i++) {
      int [] counts = nwz.get(doc[i]);
      for (int k = 0; k < numTopics; k++) {
        if (counts == null) {
          p[k] += LdaModel.LOG_MIN_PROB;
        } else {
          p[k] += 
            Math.log((counts[k] + n * beta) /
                (topicSum[k] + n * beta * nwz.size()));
        }
      }
    }
    double norm = 0.0;
    for (int i = 0; i < numTopics; i++) {
      norm += Math.exp(p[i]);
    }
    for (int i = 0; i < numTopics; i++) {
      p[i] = Math.exp(p[i]) / norm;
    }
  }
  
  protected void calculateConditionalProbability(
      String word,
      int [] ndz,
      double [] pz,
      double doclength) {
    int [] counts = nwz.get(word);
    double sum = 0.0;
    double normalizer = (doclength + numTopics * alpha - 1);
    for (int i = 0; i < numTopics; i++) {
      pz[i] = (counts[i] + beta) / (ndz[i] + nwz.size() * beta  - 1)
      * (ndz[i] + alpha) / normalizer;
      sum += pz[i];
    }
    for (int i = 0; i < numTopics; i++) {
      pz[i] /= sum;
    }
  }

  protected int sampleInDistribution(double [] dist) {
    double p = random.nextDouble();
    double sum = 0;
    for (int i = 0; i < dist.length; i++) {
      sum += dist[i];
      if (sum >= p)
        return i;
    }
    return dist.length - 1;
  }
  
  
  
  public void loadModel(String modelFile) throws IOException {
    nwz.clear();
    TextFileReader reader = null;
    if (modelFile.endsWith(".gz")) {
      reader = new GzipTextFileReader(modelFile, "UTF-8");
    } else {
      reader = new TextFileReader(modelFile, "UTF-8");
    }

    alpha = Double.parseDouble(reader.readLine());
    beta = Double.parseDouble(reader.readLine());
    numTopics = Integer.parseInt(reader.readLine());
    n = Integer.parseInt(reader.readLine());
    topicSum = new int[numTopics];
    totalSum = 0;
    
    String line;
    while ((line = reader.readLine()) != null) {
      String [] cols = line.split(" ");
      assert(cols.length == numTopics + 1);
      int [] counts = new int[numTopics + 1];
      int sum = 0;
      for (int i = 0; i < numTopics; i++) {
        counts[i] = Integer.parseInt(cols[i+1]);
        topicSum[i] += counts[i];
        sum += counts[i];
      }
      counts[numTopics] = sum;
      totalSum += sum;
      nwz.put(cols[0], counts);
      words.add(cols[0]);
    }
    reader.close();
    LOG.info("Load model parameters, alpha:" + alpha + " beta:" + beta + 
        " num_topics:" + numTopics + " num_words:" + nwz.size() + 
        " iterations:" + n);

    explanations = new String[numTopics];
    ndz = new int[numTopics];
  }

  public int getNumWords() {
    return words.size();
  }

  public String getWord(int i) {
    return words.get(i);
  }

  public int getNumTrainingIterations() {
    return n;
  }

  public double getAlpha() {
    return alpha;
  }

  public double getBeta() {
    return beta;
  }

  public double pwz(String word, int topic) {
    int [] counts = nwz.get(word);
    if (counts == null) {
      return 0.0;
    }
    return (counts[topic] + n * beta) / 
      (topicSum[topic] + n * beta * nwz.size()); 
  }

  public double pzw(int topic, String word) {
    int [] counts = nwz.get(word);
    if (counts == null) {
      return 0.0;
    }
    return (counts[topic] + n * beta) /
      (counts[numTopics] + n * beta * numTopics);     
  }

  public int getNumTopics() {
    return numTopics;
  }

  public String explain(int topic) {
    if (explanations[topic] == null) {
      String [] words = new String[5];
      double [] values = new double[5];
      int [] counts;
      for (Entry<String, int []> entry : nwz.entrySet()) {
        counts = entry.getValue();
        double pwz = (counts[topic] + n * beta) /
        (topicSum[topic] + n * beta * nwz.size());
        double pzw = (counts[topic] + n * beta) /
        (counts[numTopics] + n * beta * numTopics);
        double characteristic = Math.log(pwz + 1.0) * pzw; 
        
        for (int i = 0; i < 5; i++) {
          if (characteristic > values[i]) {
            words[i] = entry.getKey();
            values[i] = characteristic;
            break;
          }
        }
      }
      explanations[topic] = StringUtil.join(words, "-"); 
    }
    return explanations[topic];
  }
  
  public double pz(int topic) {
    return topicSum[topic] / (double)totalSum;
  }
  
  public double pw(String word) {
    int [] counts = nwz.get(word);
    if (counts == null) {
      return Math.exp(LdaModel.LOG_MIN_PROB);
    }
    return counts[numTopics] / (double)totalSum;
  }
  
  public int nw(String word) {
    int [] counts = nwz.get(word);
    if (counts == null) {
      return 0;
    }
    return counts[numTopics];
  }
  
  public double [] pzw(String word) {
    double [] p = new double[numTopics];
    pzw(word, p);
    return p;
  }

  public void pzw(String word, double [] p) {
    int [] counts = nwz.get(word);
    for (int k = 0; k < numTopics; k++) {
      if (counts == null) {
        p[k] = 1.0;
      } else {
        p[k] = (counts[k] + n * beta) / (topicSum[k] + n * beta * nwz.size());
      }
    }
    double norm = 0.0;
    for (int i = 0; i < numTopics; i++) {
      norm += p[i];
    }
    for (int i = 0; i < numTopics; i++) {
      p[i] = p[i] / norm;
    }
  }

}
