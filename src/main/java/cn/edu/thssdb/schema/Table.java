package cn.edu.thssdb.schema;

import cn.edu.thssdb.buffer.BufferPoolManager;
import cn.edu.thssdb.storage.Page;
import cn.edu.thssdb.storage.TablePage;
import cn.edu.thssdb.storage.Tuple;
import cn.edu.thssdb.utils.Global;
import cn.edu.thssdb.utils.Pair;
import cn.edu.thssdb.utils.RID;

import java.util.Iterator;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public abstract class Table {
  // TODO: concurrency control(jyx)
  protected enum NewFlag {
    INSTANCE;
  }

  protected enum OpenFlag {
    INSTANCE;
  }

  ReentrantReadWriteLock lock;
  protected final BufferPoolManager bufferPoolManager_;
  private final int firstPageId_;
  protected int slotSize_;

  // new table page needs init
  protected abstract TablePage newTablePage(Page p, int slotSize);
  // fetch does not init
  protected abstract TablePage fetchTablePage(Page p);

  protected abstract int getPageMaxTupleSize();

  // open a table
  public Table(BufferPoolManager bufferPoolManager, int firstPageId, OpenFlag flag)
      throws Exception {
    this.lock = new ReentrantReadWriteLock();
    this.bufferPoolManager_ = bufferPoolManager;
    this.firstPageId_ = firstPageId;
  }

  public Table(BufferPoolManager bufferPoolManager, int slotSize, NewFlag flag) throws Exception {
    this.lock = new ReentrantReadWriteLock();
    this.bufferPoolManager_ = bufferPoolManager;
    this.firstPageId_ = bufferPoolManager_.newPage().getPageId();
    slotSize_ = slotSize;
    // init the first page
    newTablePage(bufferPoolManager_.fetchPage(firstPageId_), slotSize);
  }

  // [out]: rid is the output parameter, return
  // the insert tuple rid
  public boolean insert(Tuple tuple, RID rid) throws Exception {
    if (tuple.getSize() > getPageMaxTupleSize()) {
      return false;
    }

    TablePage tablePage = fetchTablePage(bufferPoolManager_.fetchPage(firstPageId_));

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
        TablePage newPage = newTablePage(p, slotSize_);
        tablePage.setNextPageId(newPage.getPageId());
        newPage.setPrevPageId(tablePage.getPageId());
        bufferPoolManager_.unpinPage(tablePage.getPageId(), true);
        tablePage = newPage;
      } else {
        bufferPoolManager_.unpinPage(tablePage.getPageId(), false);
        tablePage = fetchTablePage(bufferPoolManager_.fetchPage(nextPageId));
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
    TablePage tablePage = fetchTablePage(p);
    tablePage.deleteTuple(rid.getSlotId());
    bufferPoolManager_.unpinPage(tablePage.getPageId(), true);
  }

  public boolean update(Tuple tuple, RID rid) throws Exception {
    Page p = bufferPoolManager_.fetchPage(rid.getPageId());
    if (p == null) {
      return false;
    }
    TablePage tablePage = fetchTablePage(p);
    boolean ret = tablePage.updateTuple(rid.getSlotId(), tuple);
    bufferPoolManager_.unpinPage(tablePage.getPageId(), ret);
    return ret;
  }

  // get first page
  public int getFirstPageId() {
    return firstPageId_;
  }

  // get tuple
  public Tuple getTuple(RID rid) throws Exception {
    Page p = bufferPoolManager_.fetchPage(rid.getPageId());
    if (p == null) {
      return null;
    }
    TablePage tablePage = fetchTablePage(p);
    Tuple tuple = tablePage.getTuple(rid.getSlotId());
    bufferPoolManager_.unpinPage(tablePage.getPageId(), false);
    return tuple;
  }

  protected TablePage getTablePage(int pageId) throws Exception {
    Page p = bufferPoolManager_.fetchPage(pageId);
    if (p == null) {
      return null;
    }
    return fetchTablePage(p);
  }

  private class TableIterator implements Iterator<Pair<Tuple, RID>> {
    private Iterator<Pair<Tuple, Integer>> tablePageIterator_;
    private TablePage tablePage_;

    public TableIterator() throws Exception {
      tablePage_ = getTablePage(firstPageId_);
      if (tablePage_ == null) {
        throw new Exception("TableIterator: tablePage_ is null");
      }
      tablePageIterator_ = tablePage_.iterator();
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
          tablePageIterator_ = tablePage_.iterator();
          return tablePageIterator_.hasNext();
        } catch (Exception e) {
          return false;
        }
      }
    }

    @Override
    public Pair<Tuple, RID> next() {
      Pair<Tuple, Integer> pti = tablePageIterator_.next();
      return new Pair<>(pti.left, new RID(tablePage_.getPageId(), pti.right));
    }
  }

  public Iterator<Pair<Tuple, RID>> iterator() throws Exception {
    return new TableIterator();
  }
}
