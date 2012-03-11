package org.thunlp.learning.lda;

import java.util.Arrays;
import java.util.Comparator;

import org.thunlp.misc.AnyDoublePair;
import org.thunlp.misc.Flags;
import org.thunlp.tool.GenericTool;

public class ShowTopics implements GenericTool {

  public void run(String[] args) throws Exception {
    Flags flags = new Flags();
    flags.add("model", "LDA model file");
    flags.parseAndCheck(args);

    LdaModel model = new LdaModel();
    model.loadModel(flags.getString("model"));
    AnyDoublePair<Integer> [] topics = 
      new AnyDoublePair[model.getNumTopics()];
    
    for (int i = 0; i < model.getNumTopics(); i++) {
      topics[i] = new AnyDoublePair<Integer>();
      topics[i].first = i;
      topics[i].second = model.pz(i);
    }
    
    Arrays.sort(topics, new Comparator<AnyDoublePair<Integer>>() {

      public int compare(AnyDoublePair<Integer> o1, AnyDoublePair<Integer> o2) {
        return Double.compare(o2.second, o1.second);
      }
      
    });
    
    for (int i = 0; i < topics.length; i++) {
      System.out.println(topics[i].first + " " + topics[i].second + " "
          + model.explain(topics[i].first));
    }
  }

}
