package org.thunlp.learning.lda;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.logging.Logger;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.thunlp.mapred.MapReduceJobConf;
import org.thunlp.misc.AnyDoublePair;
import org.thunlp.misc.Flags;
import org.thunlp.tool.FolderReader;
import org.thunlp.tool.FolderWriter;
import org.thunlp.tool.GenericTool;

public class InitModelTool implements GenericTool {
  private static Logger LOG = Logger.getAnonymousLogger();
  
  public void run(String[] args) throws Exception {
    Flags flags = new Flags();
    flags.add("input");
    flags.add("output_docs");
    flags.add("output_nwz");
    flags.add("num_topics");
    flags.add("wordlist");
    flags.add("max_num_words");
    flags.add("min_df");
    flags.parseAndCheck(args);
    
    Path input = new Path(flags.getString("input"));
    Path tfdf = new Path(flags.getString("wordlist") + ".tf_df");
    Path wordlist = new Path(flags.getString("wordlist"));
    int maxNumWords = flags.getInt("max_num_words");
    int minDf = flags.getInt("min_df");
    
    makeWordList(input, tfdf);
    int numWords = selectWords(tfdf, wordlist, maxNumWords, minDf);

    initModel(
        input,
        new Path(flags.getString("output_docs")),
        new Path(flags.getString("output_nwz")),
        wordlist,
        flags.getInt("num_topics"),
        numWords
        );

  }

  /**
   * Load word list, make word to id mapping.
   * All words are first sorted by their TF*IDF value. The top maxNumWords words
   * are used for training. TF*IDF is a widely used method for selecting
   * informative words in Information Retrieval, see Wikipedia for a more
   * detailed explanation. 
   * 
   * Note: words started with an underscore '_' are always kept, and they are
   * not count as number of words. This is used for special purpose.
   * 
   * @param wordFile SequenceFile of "word":"tf df".
   * @param maxNumWords How many words to keep for training, -1 means all.
   * @return number of words used.
   * @throws IOException 
   */
  public int selectWords(Path tfdf, Path wordlist, int maxNumWords, int minDf) 
  throws IOException {
    Map<String, WordFreq> wordCounts = loadWordFreq(tfdf);
    List<String> specialKeys = new LinkedList<String>();
    WordFreq total = wordCounts.get(WordListMapper.NUM_DOCS_STRING);
    if (total == null) {
      throw new RuntimeException("No number of docs key in the word list.");
    }

    List<AnyDoublePair<String>> weights = 
      new ArrayList<AnyDoublePair<String>>();
    for (Entry<String, WordFreq> e : wordCounts.entrySet()) {
      if (e.getKey().startsWith("_")) {
        specialKeys.add(e.getKey());
        continue;
      } else if (e.getKey().equals(WordListMapper.NUM_DOCS_STRING)) {
        continue;
      }
      WordFreq wf = e.getValue();
      if (wf.df > minDf) {
        double weight = wf.tf / total.tf * Math.log((total.df / wf.df));
        weights.add(new AnyDoublePair<String>(e.getKey(), weight));
      }
    }
    Collections.sort(weights, new Comparator<AnyDoublePair<String>> () {
      public int compare(AnyDoublePair<String> o1, AnyDoublePair<String> o2) {
        return Double.compare(o2.second, o1.second);
      }
    });
    FolderWriter writer = 
      new FolderWriter(wordlist, Text.class, IntWritable.class);
    Text key = new Text();
    IntWritable value = new IntWritable();
    if (maxNumWords == -1)
      maxNumWords = Integer.MAX_VALUE;
    int numWords = Math.min(maxNumWords, weights.size());
    for (int i = 0; i < numWords; i++) {
      key.set(weights.get(i).first);
      value.set(i);
      writer.append(key, value);
    }
    for (String specialKey : specialKeys) {
      key.set(specialKey);
      value.set(numWords);
      writer.append(key, value);
      numWords++;
    }
    writer.close();
    LOG.info("Load " + wordCounts.size() + " words, keep " + numWords);
    return numWords;
  }
  
  public Map<String, WordFreq> loadWordFreq(Path sqfile)
  throws IOException {
    Hashtable<String, WordFreq> keymap = new Hashtable<String, WordFreq>();
    FolderReader reader = new FolderReader(sqfile);
    Text key = new Text();
    Text value = new Text();
    while (reader.next(key, value)) {
      WordFreq wf = new WordFreq();
      String str = value.toString();
      int split = str.indexOf(' ');
      wf.tf = (double) Long.parseLong(str.substring(0, split));
      wf.df = (double) Long.parseLong(str.substring(split + 1));
      keymap.put(key.toString(), wf);
    }
    reader.close();
    return keymap;
  }

  public void makeWordList(Path input, Path output) throws IOException {
    MapReduceJobConf job = new MapReduceJobConf(this.getClass());
    job.setJobName("EstimateWordFreqForLDA");
    job.setMapReduce(WordListMapper.class, WordListReducer.class);
    job.setCombinerClass(WordListCombiner.class);
    job.setKeyValueClass(Text.class, Text.class, Text.class, Text.class);
    SequenceFileInputFormat.addInputPath(job, input);
    SequenceFileOutputFormat.setOutputPath(job, output);
    JobClient.runJob(job);
  }
  
  public void initModel(
      Path input, 
      Path outputDocs, 
      Path outputNwz,
      Path wordlist,
      int numTopics,
      int numWords) throws IOException {
    JobConf envConf = new JobConf();
    FileSystem fs = FileSystem.get(envConf);
    
    Path tmpNwz = new Path(outputNwz + "_tmp").makeQualified(fs);
    wordlist = wordlist.makeQualified(fs);
    
    MapReduceJobConf job = new MapReduceJobConf(this.getClass());
    FileSystem.get(job).mkdirs(tmpNwz);
    job.setJobName("InitializeModelForLDA");
    job.setMapReduce(InitModelMapper.class, InitModelReducer.class);
    job.setKeyValueClass(
        Text.class, DocumentWritable.class,
        Text.class, DocumentWritable.class);
    SequenceFileInputFormat.addInputPath(job, input);
    SequenceFileOutputFormat.setOutputPath(job, outputDocs);
    job.set("wordlist", wordlist.toString());
    job.set("output.nwz", tmpNwz.toString());
    job.setInt("num.topics", numTopics);
    job.setInt("num.words", numWords);
    JobClient.runJob(job);
    
    combineModelParam(tmpNwz, outputNwz);
    fs.delete(tmpNwz);
    System.out.println("Done");
  }
  
  private void combineModelParam(Path inputNwz, Path outputNwz) 
  throws IOException {
    MapReduceJobConf job = new MapReduceJobConf(this.getClass());
    job.setJobName("CombineModelParametersForLDA");
    SequenceFileInputFormat.addInputPath(job, inputNwz);
    SequenceFileOutputFormat.setOutputPath(job, outputNwz);
    job.setMapReduce(IdentityMapper.class, CombineModelParamReducer.class);
    job.setKeyValueClass(
        IntWritable.class, WordInfoWritable.class,
        IntWritable.class, WordInfoWritable.class);
    job.setBoolean("take.mean", false);
    JobClient.runJob(job);
  }
  
  private static class WordFreq {
    public double tf;
    public double df;
  }
}
