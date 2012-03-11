package org.thunlp.learning.lda;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class DocumentWritable implements Writable {
  public int [] words = null;
  public int [] topics = null;
  private int numWords = 0;
  private byte [] buffer = new byte[10240];
  
  public int getNumWords() {
    return numWords;
  }
  
  public void setNumWords( int n ) {
    if ( words == null || words.length < n ) {
      words = new int[n];
      topics = new int[n];
    }
    numWords = n;
  }

  public void readFields(DataInput input) throws IOException {
    int size = input.readInt();
    if (buffer.length < size) {
      buffer = new byte[size + 1024];
    }
    
    input.readFully(buffer, 0, size);
    setNumWords(size / 4 / 2);
    for (int i = 0; i < numWords; i++) {
      words[i] = fourBytesToInt(buffer, i * 2 * 4);
      topics[i] = fourBytesToInt(buffer, (i * 2 + 1) * 4);
    }
  }

  public void write(DataOutput output) throws IOException {
    int size = numWords * 2 * 4;
      if (buffer.length < size) {
        buffer = new byte[size + 1024];
      }
    for (int i = 0; i < numWords; i++) {
      intToFourBytes(buffer, i * 2 * 4, words[i]);
      intToFourBytes(buffer, (i * 2 + 1) * 4, topics[i]);
    }
    output.writeInt(size);
    output.write(buffer, 0, size);
  }
  
  public static int fourBytesToInt(byte [] b, int offset) {
    int i = 
      (b[offset] << 24) + 
      ((b[offset + 1] & 0xFF) << 16) +
      ((b[offset + 2] & 0xFF) << 8) +
      (b[offset + 3] & 0xFF);
    return i;
  }

  public static void intToFourBytes(byte [] b, int offset, int i) {
    b[offset] = (byte) (i >>> 24);
    b[offset + 1] = (byte) (i >>> 16);
    b[offset + 2] = (byte) (i >>> 8);
    b[offset + 3] = (byte) (i);
  }
  
  public String toString() {
    StringBuilder sb = new StringBuilder();
    for ( int i = 0 ; i < numWords ; i++ ) {
      sb.append(i > 0 ? " " : "");
      sb.append(words[i]);
      sb.append(":");
      sb.append(topics[i]);
    }
    return sb.toString();
  }

}
