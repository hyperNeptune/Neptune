package cn.edu.thssdb.storage.index;

import cn.edu.thssdb.storage.Tuple;
import cn.edu.thssdb.utils.Pair;
import cn.edu.thssdb.utils.RID;

import java.util.Iterator;

public class BPlusTreeIterator implements Iterator<Pair<Tuple, RID>> {
  @Override
  public boolean hasNext() {
    return false;
  }

  @Override
  public Pair<Tuple, RID> next() {
    return null;
  }
}
