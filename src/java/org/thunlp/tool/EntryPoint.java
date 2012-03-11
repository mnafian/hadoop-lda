package org.thunlp.tool;

import org.thunlp.learning.lda.LdaTrainer;
import org.thunlp.learning.lda.ShowTopics;

/**
 * 运行jar的入口
 */
public class EntryPoint {
  public static void main( String [] args ) throws Exception {
    if ( args.length < 1 ) {
      System.out.println("usage: train showModel");
      return;
    }

    String command = args[0];
    String [] realargs = new String[args.length - 1];
    for ( int i = 0 ; i < realargs.length ; i++ ) {
      realargs[i] = args[i+1];
    }

    GenericTool tool = null;
    if (command.equals("train")) {
      tool = new LdaTrainer();
    } else if (command.equals("showModel")) {
      tool = new ShowTopics();
    }
    tool.run(realargs);
  }
}
