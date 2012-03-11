package org.thunlp.learning.lda;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

/**
 * Combine the word topic counts from different samplers.
 * @author sixiance
 *
 */
public class CombineModelParamReducer
implements Reducer<IntWritable, WordInfoWritable,
                   IntWritable, WordInfoWritable>{
  private int [] topicCount = null;
  private int [] referenceCount = null;
  private WordInfoWritable outvalue = null;
  private boolean takeMean = true;
  
  public void reduce(IntWritable key, Iterator<WordInfoWritable> values,
      OutputCollector<IntWritable, WordInfoWritable> output, Reporter r)
      throws IOException {
    int n = 0;
    while (values.hasNext()) {
      WordInfoWritable v = values.next();
      if (topicCount == null) {
        topicCount = new int[v.size()];
        referenceCount = new int[v.size()];
        outvalue = new WordInfoWritable(v.size());
      }
      if (n == 0) {
        Arrays.fill(topicCount, 0);
        Arrays.fill(referenceCount, 0);
      }
      if (v.isPartial()) {
        for (int i = 0; i < v.size(); i++) {
          topicCount[i] += v.getTopicCount(i);
        }
      } else {
        for (int i = 0; i < v.size(); i++) {
          referenceCount[i] = v.getTopicCount(i);
        }
      }
      n++;
    }
    for (int i = 0; i < topicCount.length; i++) {
      if (takeMean) 
        outvalue.setTopicCount(i, topicCount[i] - (n-2) * referenceCount[i]);
      else
        outvalue.setTopicCount(i, topicCount[i]);
    }
    outvalue.setIsPartial(false);
    output.collect(key, outvalue);
  }

  public void configure(JobConf conf) {
    takeMean = conf.getBoolean("take.mean", true);
  }

  public void close() throws IOException {
  }
  
}
