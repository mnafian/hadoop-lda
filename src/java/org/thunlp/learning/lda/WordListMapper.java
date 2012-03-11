package org.thunlp.learning.lda;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.thunlp.misc.Counter;

public class WordListMapper implements Mapper<Text, Text, Text, Text> {
  public static String NUM_DOCS_STRING = " ";
  Text outkey = new Text();
  Text outvalue = new Text();
  Counter<String> wordfreq = new Counter<String>();
  
  public void configure(JobConf job) {
  }

  public void map(Text key, Text value, OutputCollector<Text, Text> output,
      Reporter r) throws IOException {
    String [] words = value.toString().split(" ");
    wordfreq.clear();
    for (String w : words) {
      wordfreq.inc(w, 1);
    }
    Iterator<Entry<String, Long>> iter = wordfreq.iterator();
    long numWords = 0;
    while (iter.hasNext()) {
      Entry<String, Long> entry = iter.next();
      outkey.set(entry.getKey());
      outvalue.set("d1");
      output.collect(outkey, outvalue);
      outvalue.set("t" + entry.getValue());
      output.collect(outkey, outvalue);
      numWords += entry.getValue();
    }
    outkey.set(NUM_DOCS_STRING); 
    outvalue.set("d1");
    output.collect(outkey, outvalue);
    outvalue.set("t" + numWords);
    output.collect(outkey, outvalue);  
  }

  public void close() {
  }
}


