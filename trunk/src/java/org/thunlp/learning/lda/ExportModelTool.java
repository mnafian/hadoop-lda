package org.thunlp.learning.lda;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Hashtable;
import java.util.Map;
import java.util.logging.Logger;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.thunlp.misc.Flags;
import org.thunlp.tool.FolderReader;
import org.thunlp.tool.GenericTool;

/**
 * Export the accumulated NWZs from last n iterations to an independent 
 * text file. Other software can then read the model in plain text file, which
 * will make the resulted model independent of Hadoop framework.
 * @author Xiance SI
 *
 */
public class ExportModelTool implements GenericTool {
  protected static Logger LOG = Logger.getAnonymousLogger();
  protected int [][] nwz;
  protected int numTopics;
  protected double alpha;
  protected double beta;
  protected String [] explanations;
  protected Map<Integer, String> wordIds;
  
  public void run(String[] args) throws Exception {
    Flags flags = new Flags();
    flags.add("model", "Path of model directory.");
    flags.add("output", "Output model parameter file.");
    flags.add("iterations_to_use", "Use latest n iterations.");
    flags.parseAndCheck(args);
    
    int n = flags.getInt("iterations_to_use");
    Path modelPath = new Path(flags.getString("model"));
    Path output = new Path(flags.getString("output"));
    
    exportModel(modelPath, output, n);
  }
  
  public void exportModel(Path modelPath, Path output, int n)
  throws IOException {
    wordIds = loadWords(new Path(modelPath, "words"));
    nwz = new int[wordIds.size()][];
    loadModel(modelPath, n);
    outputModelNwz(output, n);
  }
 
  public void outputModelNwz(Path output, int n) throws IOException {
    FileSystem fs = FileSystem.get(new JobConf());
    OutputStreamWriter writer = 
      new OutputStreamWriter(fs.create(output), "UTF-8");
    writer.write(alpha + "\n");
    writer.write(beta + "\n");
    writer.write(numTopics + "\n");
    writer.write(n + "\n");
    for (int w = 0; w < nwz.length; w++) {
      writer.write(wordIds.get(w));
      int [] counts = nwz[w];
      for (int i = 0; i < numTopics; i++) {
        writer.write(" ");
        writer.write(Integer.toString(counts[i]));
      }
      writer.write("\n");
    }
    writer.close();
    LOG.info("Model exported.");
  }

  /**
   * Load word to id mapping.
   */
  private Map<Integer, String> loadWords(Path path) throws IOException {
    wordIds = new Hashtable<Integer, String>();
    Map<Integer, String> keymap = new Hashtable<Integer, String>();
    FolderReader reader = new FolderReader(path);
    Text key = new Text();
    IntWritable value = new IntWritable();
    while (reader.next(key, value)) {
      keymap.put(value.get(), key.toString());
    }
    reader.close();
    return keymap;
  }
  
  public void loadModel(Path model, int n) throws IOException {
    JobConf conf = new JobConf();
    FileSystem fs = FileSystem.get(conf);

    // Load model hyper-parameters.
    Path parameters = new Path(model, "parameters");
    DataInputStream ins = fs.open(parameters);
    alpha = ins.readDouble();
    beta = ins.readDouble();
    numTopics = ins.readInt();
    ins.close();
    LOG.info("Load model parameters, alpha:" + alpha + " beta:" + beta + 
        " num_topics:" + numTopics);

    explanations = new String[numTopics];
    
    // Load last n iterations of nwz.
    Path [] files = {model};
    FileStatus [] modelFiles = fs.listStatus(files, new PathFilter() {
      public boolean accept(Path p) {
        return p.getName().contains("nwz.");
      }
    });

    Arrays.sort(modelFiles, new Comparator<FileStatus>() {
      public int compare(FileStatus p0, FileStatus p1) {
        return p1.getPath().compareTo(p0.getPath());
      }
    });

    if (modelFiles.length < n) {
      n = modelFiles.length;
    }

    for (int i = 0; i < n; i++) {
      loadNWZ(modelFiles[i].getPath());
      LOG.info("NWZ " + modelFiles[i].toString() + " loaded.");
    }
  }
  
  public void loadNWZ(Path input) throws IOException {
    IntWritable word = new IntWritable();
    WordInfoWritable topicCounts = new WordInfoWritable();
    FolderReader reader = new FolderReader(input);
    int n = 0;
    while (reader.next(word, topicCounts)) {
      int [] counts = nwz[word.get()];
      if (counts == null) {
        counts = new int[numTopics];
        Arrays.fill(counts, 0);
        nwz[word.get()] = counts;
      }
      for (int i = 0; i < numTopics; i++) {
        counts[i] += topicCounts.getTopicCount(i);
      }
      ++n;
    }
    reader.close();
  }

}
