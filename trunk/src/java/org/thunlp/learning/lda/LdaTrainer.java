package org.thunlp.learning.lda;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.logging.Logger;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.thunlp.learning.lda.PlainTextToSeqFileTool;
import org.thunlp.misc.Flags;
import org.thunlp.tool.FolderReader;
import org.thunlp.tool.GenericTool;

/**
 * The main entry for the distributed trainer.
 * @author Xiance Si (adam.si@gmail.com)
 */
public class LdaTrainer implements GenericTool {
  Logger LOG = Logger.getAnonymousLogger();
  long startTime = 0;

  public void run(String[] args) throws Exception {
    startTime = System.currentTimeMillis();
    GibbsSamplingTool sampler = new GibbsSamplingTool();
    InitModelTool initializer = new InitModelTool();

    Flags flags = new Flags();
    flags.add("input", "input documents, each is space-separated words.");
    flags.add("output", "the final model, a plain text file.");
    flags.add("working_dir", "temporary directory to hold intermediate files.");
    flags.add("num_topics", "number of topics.");
    flags.add("num_iterations", "number of iterations.");
    flags.addWithDefaultValue(
        "alpha", "-1",
        "symmetric hyper-parameter alpha. [default k/50]");
    flags.addWithDefaultValue(
        "beta", "0.01",
        "symmetric hyper-parameter beta. [default 0.01]");
    flags.addWithDefaultValue(
        "iterations_to_keep", "10",
        "number of iterations to keep on disk, " +
        "and used for final model. [default 10]");
    flags.addWithDefaultValue(
        "max_num_words", "100000",
        "max number of words to use, sorted by TF*IDF. [default 100000]");
    flags.addWithDefaultValue(
        "min_df", "5",
        "words appear in less than min_df documents " +
        "will be ignored. [default 5]");
    flags.addWithDefaultValue(
        "input_format",
        "text",
        "'sequecefile': Text value of each entry is the doc. " + 
        "'text': each line is a doc. [default 'text']");
    flags.parseAndCheck(args);

    Path input = new Path(flags.getString("input"));
    Path output = new Path(flags.getString("output"));
    Path workingDir = new Path(flags.getString("working_dir"));
    int numTopics = flags.getInt("num_topics");
    int numIterations = flags.getInt("num_iterations");
    double alpha = flags.getDouble("alpha");
    if (alpha == -1) {
      alpha = 50.0 / numTopics;
    }
    double beta = flags.getDouble("beta");
    int iterationsToKeep = flags.getInt("iterations_to_keep");
    int maxNumWords = flags.getInt("max_num_words");
    int minDf = flags.getInt("min_df");

    // Create model directory.
    JobConf conf = new JobConf();
    FileSystem fs = FileSystem.get(conf);
    if (!fs.exists(workingDir)) {
      fs.mkdirs(workingDir);
    }

    // Write model hyper-parameters to a file.
    Path parameters = new Path(workingDir, "parameters");
    if (!fs.exists(parameters)) {
      DataOutputStream out = fs.create(parameters, true /* Overwrite */);
      out.writeDouble(alpha);
      out.writeDouble(beta);
      out.writeInt(numTopics);
      out.close();
    }

    // Create likelihood file.
    OutputStreamWriter likelihoodWriter = new OutputStreamWriter(
        fs.create(new Path(workingDir, "likelihood"), true /* overwrite*/),
        "UTF-8");
    likelihoodWriter.close();

    // See if this is a previous half-done training process.
    NumberFormat formatter = new DecimalFormat("00000");
    Path [] paths = {workingDir};
    FileStatus [] existNwz = fs.listStatus(paths, new PathFilter() {
      public boolean accept(Path p) {
        logAndShow("Previous data:" + p.getName());
        return p.getName().startsWith("docs.");
      }
    });
    int latest = -1;
    for (FileStatus p : existNwz) {
      int n = Integer.parseInt(p.getPath().getName().substring(5));
      if (n > latest)
        latest = n;
    }
    if (latest >= 0) {
      logAndShow("Found previous training data at iteration #" + latest + ".");
      Path latestDocs = 
        new Path(workingDir, "docs." + formatter.format(latest));
      Path latestNwz = 
        new Path(workingDir, "nwz." + formatter.format(latest));
      if (fs.exists(latestNwz)) {
        fs.delete(latestNwz);
      }
      if (fs.exists(latestDocs)) {
        fs.delete(latestDocs);
      }
      latest--;
      logAndShow("Remove probably incomplete iteration #" + (latest + 1) +
          ", start with iteration #" + latest);
    } else {
      logAndShow("No previous data found.");
    }

    // Initialize docs and nwz.
    Path docs0 = new Path(workingDir, "docs.00000");
    Path nwz0 = new Path(workingDir, "nwz.00000");
    Path tfdf = new Path(workingDir, "tfdf");
    Path words = new Path(workingDir, "words");
    int numWords = 0;
    logAndShow("Model initialized.");

    // If we start with a plain text file, convert it to SequenceFile first.
    if (latest == -1 &&
        flags.getString("input_format").equals("text")) {
      Path seqFileInput = new Path(workingDir, "input");
      PlainTextToSeqFileTool tool = new PlainTextToSeqFileTool();
      tool.convertToSequenceFile(input, seqFileInput);
      input = seqFileInput;
      logAndShow("Text input converted to SequenceFile.");
    }

    // Initialize the model.
    if (latest == -1) {
      initializer.makeWordList(input, tfdf);
      numWords = initializer.selectWords(tfdf, words, maxNumWords, minDf);
      initializer.initModel(input, docs0, nwz0, words, numTopics, numWords);
      latest = 0;
      logAndShow("Docs initialized.");
    } else {
      numWords = loadNumWords(words);
    }

    // Begin iterations.
    for (int i = latest; i < numIterations; i++) {
      logAndShow("Begin iteration #" + (i + 1));
      Path previousDocs = new Path(workingDir, "docs." + formatter.format(i));
      Path previousNwz = new Path(workingDir, "nwz." + formatter.format(i));
      Path targetDocs = new Path(workingDir, "docs." + formatter.format(i+1));
      Path targetNwz = new Path(workingDir, "nwz." + formatter.format(i+1));
      double likelihood = sampler.sampling(
          previousDocs, targetDocs,
          previousNwz, targetNwz,
          alpha, beta, numTopics, numWords);
      logAndShow("#" + i + " Likelihood: " + likelihood);
      
      // Write likelihood. We create a new Writer each time, for that the
      // sampling process may be long, and opening a HDFS file for too long time
      // may cause strange IO problem, as observed in our practice.
      likelihoodWriter = new OutputStreamWriter(
          fs.append(new Path(workingDir, "likelihood")),
          "UTF-8");
      likelihoodWriter.append(Double.toString(likelihood));
      likelihoodWriter.append("\n");
      likelihoodWriter.close();
      // Remove unnecessary iterations.
      if (i + 1 - iterationsToKeep >= 0) {
        Path oldDocs = 
          new Path(workingDir, "docs." + formatter.format(i + 1 - iterationsToKeep)); 
        fs.delete(oldDocs);
        Path oldNWZs = 
          new Path(workingDir, "nwz." + formatter.format(i + 1 - iterationsToKeep)); 
        fs.delete(oldNWZs);        
      }
    }

    // Done training.
    likelihoodWriter.close();
    logAndShow("Training done.");

    // Export model as model file.
    logAndShow("Exporting model to model file.");
    ExportModelTool exportModelTool = new ExportModelTool();
    exportModelTool.exportModel(workingDir, output, iterationsToKeep);
    logAndShow("Model exported, thanks for using :-)  - Xiance.");
  }

  private int loadNumWords(Path words) throws IOException {
    FolderReader reader = new FolderReader(words);
    int numWords = 0;
    Text key = new Text();
    IntWritable value = new IntWritable();
    while (reader.next(key, value))
      numWords++;
    reader.close();
    return numWords;
  }

  private void logAndShow(String s) {
    System.out.println(s);
    LOG.info(s);
  }
}
