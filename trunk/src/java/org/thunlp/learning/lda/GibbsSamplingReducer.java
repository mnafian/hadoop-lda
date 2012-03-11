package org.thunlp.learning.lda;

import java.io.IOException;
import java.util.Arrays;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Random;
import java.util.Map.Entry;
import java.util.logging.Logger;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.thunlp.tool.FolderReader;

/**
 * Perform Gibbs Sampling on documents. When starting, the reducer loads p(w|z)
 * from the model file. Then it uses p(w|z) to sample topics for input documents
 * After all entries reduced, the reducer output the modified p(w|z) back to the
 * file system. 
 * @author sixiance
 *
 */
public class GibbsSamplingReducer 
implements Reducer<Text, DocumentWritable, Text, DocumentWritable>{
  private static Logger LOG = Logger.getAnonymousLogger();
  private int numTopics = 0;
  private double [] probs = null;
  private int[][] nwz = null;
  private int [] nzd = null;
  private int [] nz = null;
  private Random randomProvider = new Random();
  private double alpha = 0.0;
  private double beta = 0.0;
  private String outputNwz = null;
  private int numWords = 0;

  @Override
  public void reduce(Text key, Iterator<DocumentWritable> values,
      OutputCollector<Text, DocumentWritable> output, Reporter reporter)
  throws IOException {
    while (values.hasNext()) {
      DocumentWritable doc = values.next();
      computeNzd(doc, nzd);
      double likelihood = 0.0;
      // Sample for each word.
      for (int i = 0; i < doc.getNumWords(); i++) {
        int topic = doc.topics[i];
        int word = doc.words[i];
        nzd[topic]--;
        nz[topic]--;
        nwz[word][topic]--;
        likelihood += 
          computeSamplingProbability(nzd, word, probs, alpha, beta);
        topic = sampleInDistribution(probs, randomProvider);
        doc.topics[i] = topic;
        nzd[topic]++;
        nz[topic]++;
        nwz[word][topic]++;
      }
      reporter.incrCounter(
          GibbsSamplingTool.GibbsSamplingCounter.LIKELIHOOD,
          (long) (likelihood / doc.getNumWords() *
              GibbsSamplingTool.RESOLUTION));
      output.collect(key, doc);
    }
  }

  public void computeNzd(DocumentWritable doc, int [] ndz) {
    Arrays.fill(ndz, 0);
    for (int i = 0; i < doc.getNumWords(); i++) {
      ndz[doc.topics[i]]++;
    }
  }

  public int sampleInDistribution(double [] probs, Random random) {
    double sample = random.nextDouble();
    double sum = 0.0;
    for (int i = 0; i < probs.length; i++) {
      sum += probs[i];
      if (sample < sum) {
        return i;
      }
    }
    return probs.length - 1;
  }

  public double computeSamplingProbability(
      int [] nzd, 
      int word, 
      double [] probs,
      double alpha,
      double beta) {
    Arrays.fill(probs, 0.0);
    double norm = 0.0;
    double dummyNorm = 1.0;
    for (int i = 0; i < numTopics; i++) {
      double pwz = (nwz[word][i] + beta) / (nz[i] + nwz.length * beta);
      double pzd = (nzd[i] + alpha) / dummyNorm;
      probs[i] = pwz * pzd;
      norm += probs[i];
    }
    for (int i = 0; i < numTopics; i++) {
      probs[i] /= norm;
    }
    return norm;
  }
  
  public double sum(int [] values) {
    double sum = 0;
    for (double v : values) {
      sum += v;
    }
    return sum; 
  }

  public void configure(JobConf conf) {
    numTopics = conf.getInt("num.topics", 0);
    numWords = conf.getInt("num.words", 0);
    probs = new double[numTopics];
    nzd = new int[numTopics];
    nz = new int[numTopics];
    outputNwz = conf.get("output.nwz");
    alpha = (double) conf.getFloat("alpha", 0.0f);
    beta = (double) conf.getFloat("beta", 0.0f);
    
    try {
      loadModelParameters(conf.get("input.nwz"));
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  private void loadModelParameters(String modelParamFile) throws IOException {
    long startTime = System.currentTimeMillis();
    Path modelParamPath = new Path(modelParamFile);
    FolderReader fr = new FolderReader(modelParamPath);
    IntWritable key = new IntWritable();
    WordInfoWritable value = new WordInfoWritable(numTopics);
    Arrays.fill(nz, 0);
    nwz = new int[numWords][];
    while (fr.next(key, value)) {
      int [] count = new int[numTopics];
      for (int i = 0; i < numTopics; i++) {
        count[i] = value.getTopicCount(i);
        nz[i] += count[i];
      }
      nwz[key.get()] = count;
    }
    // Ensure each word has its count.
    for (int i = 0; i < numWords; i++) {
      if (nwz[i] == null) {
        nwz[i] = new int[numTopics];
      }
    }
    fr.close();
    long duration = System.currentTimeMillis() - startTime;
    LOG.info("Load model parameters using " + duration + " milliseconds.");
  }

  /**
   * @param modelParamPart
   * @throws IOException
   */
  private void saveModelParameters(String modelParamPart) throws IOException {
    long startTime = System.currentTimeMillis();
    JobConf envConf = new JobConf();
    SequenceFile.Writer writer = SequenceFile.createWriter(
        FileSystem.get(envConf), 
        envConf, 
        new Path(modelParamPart), 
        IntWritable.class, 
        WordInfoWritable.class);
    IntWritable key = new IntWritable();
    WordInfoWritable value = new WordInfoWritable(numTopics);
    for (int i = 0; i < nwz.length; i++) {
      key.set(i);
      for (int j = 0; j < numTopics; j++) {
        value.setTopicCount(j, nwz[i][j]); 
      }
      value.setIsPartial(true);
      writer.append(key, value);
    }
    writer.close();
    long duration = System.currentTimeMillis() - startTime;
    LOG.info("Save model parameters using " + duration + " milliseconds.");
  }

  public void close() throws IOException {
    String partName = "part-" + Math.abs(randomProvider.nextInt());
    saveModelParameters(outputNwz + "/" + partName); 
  }

}
