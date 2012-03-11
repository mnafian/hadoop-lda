package org.thunlp.learning.lda;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.thunlp.tool.FolderReader;

/**
 * Convert space-separated words to DocumentWritable.
 * Key in&out: document id(not used).
 * Value in: Space-separated words of a document.
 * Value out: A DocumentWritable contains exactly the input document.
 * @author sixiance
 *
 */
public class InitModelMapper 
implements Mapper<Text, Text, Text, DocumentWritable> {
  private static Logger LOG = Logger.getAnonymousLogger();
  Map<String, Integer> wordmap = null;
  DocumentWritable doc = new DocumentWritable();
  List<Integer> wordbuf = new ArrayList<Integer>();
  
  public void map(Text key, Text value,
      OutputCollector<Text, DocumentWritable> output, Reporter r)
  throws IOException {
    String [] words = value.toString().split(" +");
    wordbuf.clear(); 
    for (int i = 0; i < words.length; i++) {
      Integer id = wordmap.get(words[i]);
      if (id == null) {
        continue;
      }
      wordbuf.add(id);
    }
    doc.setNumWords(wordbuf.size());
    for (int i = 0; i < wordbuf.size(); i++) {
      doc.words[i] = wordbuf.get(i);
    }
    output.collect(key, doc);
  }

  public void configure(JobConf conf) {
    try {
      wordmap = loadWordList(conf.get("wordlist"));
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
  
  public void setWordList(Hashtable<String, Integer> wordmap) {
    this.wordmap = wordmap;
  }

  /**
   * Load word to id mapping.
   */
  private Map<String, Integer> loadWordList(String wordFile)
  throws IOException {
    Hashtable<String, Integer> keymap = new Hashtable<String, Integer>();
    FolderReader reader = new FolderReader(new Path(wordFile));
    Text key = new Text();
    IntWritable value = new IntWritable();
    while (reader.next(key, value)) {
      keymap.put(key.toString(), value.get());
    }
    reader.close();
    return keymap;
  }

  public void close() throws IOException {
  }

}
