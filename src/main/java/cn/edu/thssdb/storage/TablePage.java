package cn.edu.thssdb.storage;

import cn.edu.thssdb.schema.Schema;

public interface TablePage {
  // next page, prev page
  public int getNextPageId();

  public int getPrevPageId();

  public void setNextPageId(int nextPageId);

  public void setPrevPageId(int prevPageId);

  public int insertTuple(Tuple tuple);

  public boolean deleteTuple(int slotId);

  public Tuple getTuple(int slotId, Schema schema);

  public boolean updateTuple(int slotId, Tuple tuple);
}
