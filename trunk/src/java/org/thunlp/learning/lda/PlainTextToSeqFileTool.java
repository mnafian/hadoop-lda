package org.thunlp.learning.lda;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.thunlp.misc.Flags;
import org.thunlp.tool.GenericTool;

public class PlainTextToSeqFileTool implements GenericTool {

  @Override
  public void run(String[] args) throws Exception {
    Flags flags = new Flags();
    flags.add("input");
    flags.add("output");
    flags.parseAndCheck(args);
    
    convertToSequenceFile(
        new Path(flags.getString("input")),
        new Path(flags.getString("output")));
  }

  public void convertToSequenceFile(Path input, Path output)
  throws IOException {
    JobConf job = new JobConf();
    job.setJarByClass(this.getClass());
    job.setJobName("text-to-sequence-file");
    job.setMapperClass(ConvertMapper.class);
    job.setReducerClass(IdentityReducer.class);
    job.setNumReduceTasks(0);
    job.setInputFormat(TextInputFormat.class);
    job.setOutputFormat(SequenceFileOutputFormat.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    TextInputFormat.addInputPath(job, input);
    SequenceFileOutputFormat.setOutputPath(job, output);
    JobClient.runJob(job);
  }
  
  public static class ConvertMapper
  implements Mapper<LongWritable, Text, Text, Text> {
    Text outkey = new Text();
    Text outvalue = new Text();

    public void configure(JobConf job) {
    }

    public void map(LongWritable key, Text value,
        OutputCollector<Text, Text> collector, Reporter r) throws IOException {
      outkey.set(Long.toString(key.get()));
      collector.collect(outkey, value);
    }

    public void close() {
    }
  }
}
