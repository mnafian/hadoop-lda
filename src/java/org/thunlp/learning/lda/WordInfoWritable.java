package org.thunlp.learning.lda;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * This class carries the information about a word, including distribution over
 * all latent topics and the probability of sampling this word in this iteration.
 * @author sixiance
 *
 */
public class WordInfoWritable implements Writable {
  protected int [] topicCount;
  protected byte [] buffer;
  protected boolean isPartial;
  
  public WordInfoWritable(int n) {
    topicCount = new int[n];
    buffer = new byte[n * 4];
    isPartial = false;
  }
  
  public WordInfoWritable() {
    topicCount = null;
    isPartial = false;
  }

  public void setIsPartial(boolean isPartial) {
    this.isPartial = isPartial;
  }
  
  public boolean isPartial() {
    return isPartial;
  }
  
  public void setTopicCount(int i, int v) {
    topicCount[i] = v;
  }
  
  public int getTopicCount(int i) {
    return topicCount[i];
  }
  
  public int size() {
    return topicCount.length;
  }
  
  public void readFields(DataInput in) throws IOException {
    int size = in.readInt();
    if (topicCount == null || size != topicCount.length) {
      topicCount = new int[size];
      buffer = new byte[size * 4];
    }
    in.readFully(buffer);
    
    for (int i = 0; i < topicCount.length; i++) {
      topicCount[i] = DocumentWritable.fourBytesToInt(buffer, i * 4);
    }
    isPartial = in.readBoolean();
  }

  public void write(DataOutput out) throws IOException {
    out.writeInt(topicCount.length);
    for (int i = 0; i < topicCount.length; i++) {
      DocumentWritable.intToFourBytes(buffer, i*4, topicCount[i]);
    }
    out.write(buffer);
    out.writeBoolean(isPartial);
  }
  
  public String toString() {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < topicCount.length; i++) {
      sb.append(i);
      sb.append(":");
      sb.append(topicCount[i]);
      sb.append(" ");
    }
    return sb.toString();
  }

}
