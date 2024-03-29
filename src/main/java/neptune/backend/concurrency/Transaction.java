package neptune.backend.concurrency;

import neptune.backend.storage.Page;
import neptune.common.RID;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

enum WType {
  INSERT,
  DELETE,
  UPDATE
}

public class Transaction {
  private final int txn_id;
  private final long thread_id;
  private int prev_lsn; // the LSN of the last record written by the transaction
  private TransactionState state = TransactionState.GROWING;
  private IsolationLevel isolationLevel;
  private final HashSet<String> s_table_lock_set;
  private final HashSet<String> x_table_lock_set;
  private final HashMap<String, HashSet<RID>> s_row_lock_set;
  private final HashMap<String, HashSet<RID>> x_row_lock_set;
  private final ReentrantLock latch = new ReentrantLock();
  private final List<Page> pageSet = new ArrayList<>();
  private final ArrayList<TableWriteRecord> table_write_set = new ArrayList<>();

  public Transaction(int txn_id, IsolationLevel iso_lvl) {
    this.txn_id = txn_id;
    this.isolationLevel = iso_lvl;
    this.thread_id = Thread.currentThread().getId();
    this.prev_lsn = -1;
    s_table_lock_set = new HashSet<>();
    x_table_lock_set = new HashSet<>();
    s_row_lock_set = new HashMap<>();
    x_row_lock_set = new HashMap<>();
  }

  public TransactionState getState() {
    return state;
  }

  public void setState(TransactionState state) {
    this.state = state;
  }

  public int getPrev_lsn() {
    return prev_lsn;
  }

  public void setPrev_lsn(int lsn) {
    this.prev_lsn = lsn;
  }

  public int getTxn_id() {
    return txn_id;
  }

  public IsolationLevel getIsolationLevel() {
    return isolationLevel;
  }

  public boolean IsTableSharedLocked(String tableName) {
    return s_table_lock_set.contains(tableName);
  }

  public boolean IsTableExclusiveLocked(String tableName) {
    return x_table_lock_set.contains(tableName);
  }

  public boolean IsRowSharedLocked(String tableName, RID rid) {
    HashSet<RID> ridSet = s_row_lock_set.get(tableName);
    if (ridSet == null) {
      return false;
    } else {
      return ridSet.contains(rid);
    }
  }

  public boolean IsRowExclusiveLocked(String tableName, RID rid) {
    HashSet<RID> ridSet = x_row_lock_set.get(tableName);
    if (ridSet == null) {
      return false;
    } else {
      return ridSet.contains(rid);
    }
  }

  public void lockTxn() {
    latch.lock();
  }

  public void unlockTxn() {
    latch.unlock();
  }

  public void setIsolationLevel(IsolationLevel isolationLevel) {
    this.isolationLevel = isolationLevel;
  }

  public HashSet<String> getSharedTableLockSet() {
    return s_table_lock_set;
  }

  public HashSet<String> getExclusiveTableLockSet() {
    return x_table_lock_set;
  }

  public HashMap<String, HashSet<RID>> getSharedRowLockSet() {
    return s_row_lock_set;
  }

  public HashMap<String, HashSet<RID>> getExclusiveRowLockSet() {
    return x_row_lock_set;
  }

  public List<Page> getPageSet() {
    return pageSet;
  }

  public ArrayList<TableWriteRecord> getTableWriteSet() {
    return table_write_set;
  }

  public void addTableWriteRecord() {}

  public long getThread_id() {
    return thread_id;
  }

  public static int INVALID_TXN_ID = -1;
}
