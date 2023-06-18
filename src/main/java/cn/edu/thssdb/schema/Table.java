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
    INSTANCE
  }

  protected enum OpenFlag {
    INSTANCE
  }

  ReentrantReadWriteLock lock;
  protected final BufferPoolManager bufferPoolManager_;
  private final int firstPageId_;
  protected int slotSize_;
  private int insertCurPage = 0;

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
    insertCurPage = firstPageId;
  }

  public Table(BufferPoolManager bufferPoolManager, int slotSize, NewFlag flag) throws Exception {
    this.lock = new ReentrantReadWriteLock();
    this.bufferPoolManager_ = bufferPoolManager;
    this.firstPageId_ = bufferPoolManager_.newPage().getPageId();
    insertCurPage = firstPageId_;
    slotSize_ = slotSize;
    // init the first page
    newTablePage(bufferPoolManager_.fetchPage(firstPageId_), slotSize);
    bufferPoolManager_.unpinPage(firstPageId_, true);
  }

  // [out]: rid is the output parameter, return
  // the insert tuple rid
  public boolean insert(Tuple tuple, RID rid) throws Exception {
    if (tuple.getSize() > getPageMaxTupleSize()) {
      return false;
    }

    TablePage tablePage = fetchTablePage(bufferPoolManager_.fetchPage(insertCurPage));
    tablePage.WLock();

    // invariant: insertTuple failed, tablePage hold WLock
    // rid is changed in insertTuple
    while (tablePage.insertTuple(tuple, rid) == -1) {
      int nextPageId = tablePage.getNextPageId();
      if (nextPageId == Global.PAGE_ID_INVALID) {
        Page p = bufferPoolManager_.newPage();
        if (p == null) {
          tablePage.WUnlock();
          bufferPoolManager_.unpinPage(tablePage.getPageId(), false);
          return false;
        }
        TablePage newPage = newTablePage(p, slotSize_);
        newPage.WLock();
        tablePage.setNextPageId(newPage.getPageId());
        newPage.setPrevPageId(tablePage.getPageId());
        tablePage.WUnlock();
        bufferPoolManager_.unpinPage(tablePage.getPageId(), true);
        tablePage = newPage;
      } else {
        TablePage nextPage = fetchTablePage(bufferPoolManager_.fetchPage(nextPageId));
        nextPage.WLock();
        tablePage.WUnlock();
        bufferPoolManager_.unpinPage(tablePage.getPageId(), false);
        tablePage = nextPage;
      }
    }

    tablePage.WUnlock();
    // unpin
    bufferPoolManager_.unpinPage(tablePage.getPageId(), true);
    insertCurPage = tablePage.getPageId();
    return true;
  }

  public void delete(RID rid) throws Exception {
    Page p = bufferPoolManager_.fetchPage(rid.getPageId());
    if (p == null) {
      return;
    }
    TablePage tablePage = fetchTablePage(p);
    tablePage.WLock();
    tablePage.deleteTuple(rid.getSlotId());
    tablePage.WUnlock();
    bufferPoolManager_.unpinPage(tablePage.getPageId(), true);
  }

  public boolean update(Tuple tuple, RID rid) throws Exception {
    Page p = bufferPoolManager_.fetchPage(rid.getPageId());
    if (p == null) {
      return false;
    }
    TablePage tablePage = fetchTablePage(p);
    tablePage.WLock();
    boolean ret = tablePage.updateTuple(rid.getSlotId(), tuple);
    tablePage.WUnlock();
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
    tablePage.RLock();
    Tuple tuple = tablePage.getTuple(rid.getSlotId());
    tablePage.RUnlock();
    bufferPoolManager_.unpinPage(tablePage.getPageId(), false);
    return tuple;
  }

  // page is pinned
  protected TablePage getTablePage(int pageId) throws Exception {
    Page p = bufferPoolManager_.fetchPage(pageId);
    if (p == null) {
      return null;
    }
    return fetchTablePage(p);
  }

  private class TableIterator implements Iterator<Pair<Tuple, RID>> {
    private Iterator<Pair<Tuple, Integer>> tablePageIterator_;
    // this page is pinned in this class
    private TablePage tablePage_;
    private RID curRid_;

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
          bufferPoolManager_.unpinPage(tablePage_.getPageId(), false);
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
      tablePage_.RLock();
      Pair<Tuple, Integer> pti = tablePageIterator_.next();
      curRid_ = new RID(tablePage_.getPageId(), pti.right);
      tablePage_.RUnlock();
      return new Pair<>(pti.left, curRid_);
    }

    @Override
    public void remove() {
      tablePage_.deleteTuple(curRid_.getSlotId());
    }
  }

  public Iterator<Pair<Tuple, RID>> iterator() throws Exception {
    return new TableIterator();
  }
}
