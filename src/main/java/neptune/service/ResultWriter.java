package neptune.service;

import neptune.schema.Column;
import neptune.schema.Schema;
import neptune.storage.Tuple;

import java.util.ArrayList;
import java.util.List;

// this class is for writing the result of a query to a string
public class ResultWriter {
  public static List<String> writeHDR(Schema sh) {
    List<String> ls = new ArrayList<>();
    for (Column c : sh.getColumns()) {
      ls.add(c.getName());
    }
    return ls;
  }

  public static List<List<String>> writeTBL(List<Tuple> tuples, Schema sh) {
    List<List<String>> lls = new ArrayList<>();

    for (int i = 0; i < tuples.size(); i++) {
      List<String> ls = new ArrayList<>();
      Tuple t = tuples.get(i);
      for (int j = 0; j < sh.getColumns().length; j++) {
        ls.add(t.getValue(sh, j).toString());
      }
      lls.add(ls);
    }
    return lls;
  }
}
