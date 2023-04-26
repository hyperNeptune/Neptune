package cn.edu.thssdb.storage;

import cn.edu.thssdb.schema.Schema;
import cn.edu.thssdb.utils.RID;

public interface TablePage {
  public int getPageId();
  // next page, prev page
  public int getNextPageId();

  public int getPrevPageId();

  public void setNextPageId(int nextPageId);

  public void setPrevPageId(int prevPageId);

  public int insertTuple(Tuple tuple, RID rid);

  public boolean deleteTuple(int slotId);

  public Tuple getTuple(int slotId, Schema schema);

  public boolean updateTuple(int slotId, Tuple tuple);

  public void init(Object[] data);
}
