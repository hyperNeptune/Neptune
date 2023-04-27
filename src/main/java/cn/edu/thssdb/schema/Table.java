package cn.edu.thssdb.schema;

import cn.edu.thssdb.buffer.BufferPoolManager;
import cn.edu.thssdb.storage.Page;
import cn.edu.thssdb.storage.TablePage;
import cn.edu.thssdb.storage.TablePageSlot;
import cn.edu.thssdb.storage.Tuple;
import cn.edu.thssdb.utils.Global;
import cn.edu.thssdb.utils.RID;

import java.util.Iterator;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Table {
  // TODO: concurrency control(jyx)
  ReentrantReadWriteLock lock;
  private final BufferPoolManager bufferPoolManager_;
  private final int firstPageId_;

  // open a table
  public Table(BufferPoolManager bufferPoolManager, int firstPageId) {
    this.lock = new ReentrantReadWriteLock();
    this.bufferPoolManager_ = bufferPoolManager;
    this.firstPageId_ = firstPageId;
  }

  // create a table
  public Table(BufferPoolManager bufferPoolManager, Schema sh) throws Exception {
    this.lock = new ReentrantReadWriteLock();
    this.bufferPoolManager_ = bufferPoolManager;
    this.firstPageId_ = bufferPoolManager_.newPage().getPageId();
    // init the first page
    TablePage tablePage = new TablePageSlot(bufferPoolManager_.fetchPage(firstPageId_));
    Object[] data = new Object[1];
    data[0] = sh.getSize();
    tablePage.init(data);
  }

  // [out]: rid is the output parameter, return
  // the insert tuple rid
  public boolean insert(Tuple tuple, RID rid) throws Exception {
    if (tuple.getSize() > Global.PAGE_SIZE - TablePageSlot.PAGE_HEADER_SIZE - 1) {
      return false;
    }

    TablePage tablePage = new TablePageSlot(bufferPoolManager_.fetchPage(firstPageId_));

    // invariant: insertTuple failed
    // rid is changed in insertTuple
    while (tablePage.insertTuple(tuple, rid) == -1) {
      int nextPageId = tablePage.getNextPageId();
      if (nextPageId == Global.PAGE_ID_INVALID) {
        Page p = bufferPoolManager_.newPage();
        if (p == null) {
          bufferPoolManager_.unpinPage(tablePage.getPageId(), false);
          return false;
        }
        TablePageSlot newPage = new TablePageSlot(p);
        tablePage.setNextPageId(newPage.getPageId());
        newPage.setPrevPageId(tablePage.getPageId());
        Object[] d = new Object[1];
        d[0] = tuple.getSize();
        newPage.init(d);
        bufferPoolManager_.unpinPage(tablePage.getPageId(), true);
        tablePage = newPage;
      } else {
        bufferPoolManager_.unpinPage(tablePage.getPageId(), false);
        tablePage = new TablePageSlot(bufferPoolManager_.fetchPage(nextPageId));
      }
    }

    // unpin
    bufferPoolManager_.unpinPage(tablePage.getPageId(), true);
    return true;
  }

  public void delete(RID rid) throws Exception {
    Page p = bufferPoolManager_.fetchPage(rid.getPageId());
    if (p == null) {
      return;
    }
    TablePage tablePage = new TablePageSlot(p);
    tablePage.deleteTuple(rid.getSlotId());
    bufferPoolManager_.unpinPage(tablePage.getPageId(), true);
  }

  public boolean update(Tuple tuple, RID rid) throws Exception {
    Page p = bufferPoolManager_.fetchPage(rid.getPageId());
    if (p == null) {
      return false;
    }
    TablePage tablePage = new TablePageSlot(p);
    boolean ret = tablePage.updateTuple(rid.getSlotId(), tuple);
    bufferPoolManager_.unpinPage(tablePage.getPageId(), ret);
    return ret;
  }

  // get first page
  public int getFirstPageId() {
    return firstPageId_;
  }

  // get tuple
  public Tuple getTuple(RID rid, Schema sh) throws Exception {
    Page p = bufferPoolManager_.fetchPage(rid.getPageId());
    if (p == null) {
      return null;
    }
    TablePage tablePage = new TablePageSlot(p);
    Tuple tuple = tablePage.getTuple(rid.getSlotId(), sh);
    bufferPoolManager_.unpinPage(tablePage.getPageId(), false);
    return tuple;
  }

  protected TablePage getTablePage(int pageId) throws Exception {
    Page p = bufferPoolManager_.fetchPage(pageId);
    if (p == null) {
      return null;
    }
    return new TablePageSlot(p);
  }

  private class TableIterator implements Iterator<Tuple> {
    private Iterator<Tuple> tablePageIterator_;
    private TablePage tablePage_;
    private final Schema schema_;

    public TableIterator(Schema schema) throws Exception {
      schema_ = schema;
      tablePage_ = getTablePage(firstPageId_);
      if (tablePage_ == null) {
        throw new Exception("TableIterator: tablePage_ is null");
      }
      tablePageIterator_ = tablePage_.iterator(schema_);
    }

    @Override
    public boolean hasNext() {
      if (tablePageIterator_.hasNext()) {
        return true;
      } else {
        try {
          int nextPageId = tablePage_.getNextPageId();
          if (nextPageId == Global.PAGE_ID_INVALID) {
            return false;
          }
          tablePage_ = getTablePage(nextPageId);
          if (tablePage_ == null) {
            return false;
          }
          tablePageIterator_ = tablePage_.iterator(schema_);
          return tablePageIterator_.hasNext();
        } catch (Exception e) {
          return false;
        }
      }
    }

    @Override
    public Tuple next() {
      return tablePageIterator_.next();
    }
  }

  public Iterator<Tuple> iterator(Schema sh) throws Exception {
    return new TableIterator(sh);
  }
}
