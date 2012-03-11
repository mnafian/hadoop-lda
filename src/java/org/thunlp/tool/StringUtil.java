package org.thunlp.tool;

import java.util.Collection;

public class StringUtil {
  public static String join(Collection<String> strings, String delimiter) {
    StringBuilder sb = new StringBuilder();
    boolean first = true;
    for (String s : strings) {
      if (!first) {
        sb.append(delimiter);
      }
      sb.append(s);
      first = false;
    }
    return sb.toString();
  }
  
  public static String join(String [] strings, String delimiter) {
    StringBuilder sb = new StringBuilder();
    boolean first = true;
    for (String s : strings) {
      if (!first) {
        sb.append(delimiter);
      }
      sb.append(s);
      first = false;
    }
    return sb.toString();
  }
  
  public static String join(Object [] objects, String delimiter) {
    StringBuilder sb = new StringBuilder();
    boolean first = true;
    for (Object o : objects) {
      if (!first) {
        sb.append(delimiter);
      }
      sb.append(o.toString());
      first = false;
    }
    return sb.toString();
  }
}
