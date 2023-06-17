package cn.edu.thssdb.storage;

import cn.edu.thssdb.utils.Pair;
import cn.edu.thssdb.utils.RID;

import java.util.Iterator;

public interface TablePage {
  public int getPageId();
  // next page, prev page
  public int getNextPageId();

  public int getPrevPageId();

  public void setNextPageId(int nextPageId);

  public void setPrevPageId(int prevPageId);

  public int insertTuple(Tuple tuple, RID rid);

  public boolean deleteTuple(int slotId);

  public Tuple getTuple(int slotId);

  public boolean updateTuple(int slotId, Tuple tuple);

  public void init(Object[] data);

  public int getSlotSize();

  public Iterator<Pair<Tuple, Integer>> iterator();

  // concurrency control stuff
  public void RLock();

  public void RUnlock();

  public void WLock();

  public void WUnlock();
}
