package org.thunlp.learning.lda;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class WordListReducer 
implements Reducer<Text, Text, Text, Text> {
  Text outvalue = new Text();
  
  public void reduce(Text key, Iterator<Text> values,
      OutputCollector<Text, Text> output, Reporter r) throws IOException {
    long tf = 0;
    long df = 0;
    while (values.hasNext()) {
      String value = values.next().toString();
      if (value.charAt(0) == 'd') {
        df += Long.parseLong(value.substring(1));
      } else if (value.charAt(0) == 't') {
        tf += Long.parseLong(value.substring(1));
      }
    }
    outvalue.set(tf + " " + df);
    output.collect(key, outvalue);
  }

  public void configure(JobConf conf) {}

  public void close() throws IOException {}

}
