package org.thunlp.learning.lda;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.thunlp.mapred.MapReduceJobConf;
import org.thunlp.misc.Flags;
import org.thunlp.tool.GenericTool;

/**
 * Perform Gibbs Sampling on a set of documents, according to the NWZ file.
 * First, we pass the documents to GibbsSamplingReducer by IdentityMapper. Then,
 * GibbsSamplingReducer do the sampling, output the documents with new topic
 * assignmentsm, and also output the changed NWZ file. Finally, another
 * map-reduce combines the NWZ files from different reducers into one.
 * 
 * The reason of not doing sampling in the map stage is efficiency. We have to
 * load a possibly large NWZ file into memory before sampling, which may take a
 * lot of time. Normally Hadoop allocates one reducer and several mappers for 
 * one machine. If we do the sampling in the map stage, the same NWZ-loading
 * work would be repeated several times on one machine, which is a waste of
 * resource and significantly slows down the whole training process.
 * @author sixiance
 *
 */
public class GibbsSamplingTool implements GenericTool {
  public enum GibbsSamplingCounter {LIKELIHOOD};
  public static double RESOLUTION = 0.01;
  
  public void run(String[] args) throws Exception {
    Flags flags = new Flags();
    flags.add("input_docs");
    flags.add("input_nwz");
    flags.add("output_docs");
    flags.add("output_nwz");
    flags.add("alpha");
    flags.add("beta");
    flags.add("num_topics");
    flags.add("num_words");
    flags.parseAndCheck(args);
  
    double likelihood = sampling(
        new Path(flags.getString("input_docs")),
        new Path(flags.getString("output_docs")),
        new Path(flags.getString("input_nwz")),
        new Path(flags.getString("output_nwz")),
        flags.getDouble("alpha"),
        flags.getDouble("beta"),
        flags.getInt("num_topics"),
        flags.getInt("num_words")
        );

    System.out.println("Done with likelihood " + likelihood);
  }
  
  /**
   * Do Gibbs Sampling on the document set, and return the overall likelihood.
   * @param inputDocs
   * @param outputDocs
   * @param inputNwz
   * @param outputNwz
   * @param alpha
   * @param beta
   * @param numWords
   * @param numTopics
   * @return Overall likelihood.
   * @throws IOException
   */
  public double sampling(
      Path inputDocs, Path outputDocs,
      Path inputNwz, Path outputNwz,
      double alpha, double beta,
      int numTopics, int numWords) 
  throws IOException {
    JobConf envConf = new JobConf();
    FileSystem fs = FileSystem.get(envConf);
    
    Path tmpNwz = new Path(outputNwz + "_tmp").makeQualified(fs);
    inputNwz = inputNwz.makeQualified(fs);
    
    MapReduceJobConf job = new MapReduceJobConf(this.getClass());
    FileSystem.get(job).mkdirs(tmpNwz);
    job.setJobName("GibbsSamplingForLDA");
    SequenceFileInputFormat.addInputPath(job, inputDocs);
    SequenceFileOutputFormat.setOutputPath(job, outputDocs);
    job.set("input.nwz", inputNwz.toString());
    job.set("output.nwz", tmpNwz.toString());
    job.set("alpha", Double.toString(alpha));
    job.set("beta", Double.toString(beta));
    job.set("num.topics", Integer.toString(numTopics));
    job.set("num.words", Integer.toString(numWords));
    job.setMapReduce(IdentityMapper.class, GibbsSamplingReducer.class);
    job.setKeyValueClass(
        Text.class, DocumentWritable.class,
        Text.class, DocumentWritable.class);
    RunningJob runningJob = JobClient.runJob(job);
    runningJob.waitForCompletion();
    double likelihood = 
      runningJob.getCounters().getCounter(GibbsSamplingCounter.LIKELIHOOD) /
      GibbsSamplingTool.RESOLUTION;
    
    combineModelParam(inputNwz, tmpNwz, outputNwz);
    fs.delete(tmpNwz);
    
    return likelihood;
  }
  
  private void combineModelParam(Path refNwz, Path inputNwz, Path outputNwz) 
  throws IOException {
    MapReduceJobConf job = new MapReduceJobConf(this.getClass());
    job.setJobName("CombineModelParametersForLDA");
    SequenceFileInputFormat.addInputPath(job, inputNwz);
    SequenceFileInputFormat.addInputPath(job, refNwz);
    SequenceFileOutputFormat.setOutputPath(job, outputNwz);
    job.setMapReduce(IdentityMapper.class, CombineModelParamReducer.class);
    job.setKeyValueClass(
        IntWritable.class, WordInfoWritable.class,
        IntWritable.class, WordInfoWritable.class);
    JobClient.runJob(job);
  }

}
