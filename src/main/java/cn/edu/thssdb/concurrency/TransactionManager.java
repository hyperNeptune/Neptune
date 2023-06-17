package cn.edu.thssdb.concurrency;

import cn.edu.thssdb.recovery.LogManager;
import cn.edu.thssdb.recovery.LogRecord;
import cn.edu.thssdb.recovery.LogRecordType;
import cn.edu.thssdb.storage.Tuple;
import cn.edu.thssdb.utils.RID;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

public class TransactionManager {
  private final LockManager lockManager;
  private final LogManager logManager;

  private final HashMap<Integer, Transaction> txn_map = new HashMap<>();
  private final ReentrantLock txn_map_latch = new ReentrantLock();

  private int currentTxnID;
  private final ReentrantLock txn_id_latch = new ReentrantLock();

  public TransactionManager(LogManager logManager, LockManager lockManager) {
    this.logManager = logManager;
    this.currentTxnID = 0;
    this.lockManager = lockManager;
  }

  public Transaction create_and_begin(IsolationLevel isolationLevel) {
    txn_id_latch.lock();
    currentTxnID++;
    int id = currentTxnID;
    txn_id_latch.unlock();

    Transaction txn = new Transaction(id, isolationLevel);
    return begin(txn);
  }

  public Transaction begin(Transaction txn) {
    LogRecord record = new LogRecord(txn.getTxn_id(), txn.getPrev_lsn(), LogRecordType.BEGIN);
    int lsn = logManager.appendLogRecord(record);
    txn.setPrev_lsn(lsn);

    txn_map_latch.lock();
    txn_map.put(txn.getTxn_id(), txn);
    txn_map_latch.unlock();

    return txn;
  }

  private void releaseAllLocks(Transaction txn) {
    txn.lockTxn();
    HashMap<String, HashSet<RID>> mergedRowLockSet = new HashMap<>();
    for (String tableName : txn.getExclusiveRowLockSet().keySet()) {
      HashSet<RID> ridSet = txn.getExclusiveRowLockSet().get(tableName);
      HashSet<RID> newRidSet = new HashSet<>(ridSet);
      mergedRowLockSet.put(tableName, newRidSet);
    }
    for (String tableName : txn.getSharedRowLockSet().keySet()) {
      HashSet<RID> ridSet = txn.getSharedRowLockSet().get(tableName);
      if (mergedRowLockSet.get(tableName) == null) {
        HashSet<RID> newRidSet = new HashSet<>(ridSet);
        mergedRowLockSet.put(tableName, newRidSet);
      } else {
        for (RID rid : ridSet) {
          mergedRowLockSet.get(tableName).add(rid);
        }
      }
    }
    HashSet<String> mergedTableLockSet = new HashSet<>();
    mergedTableLockSet.addAll(txn.getExclusiveTableLockSet());
    mergedTableLockSet.addAll(txn.getSharedTableLockSet());
    txn.unlockTxn();

    for (Map.Entry<String, HashSet<RID>> entry : mergedRowLockSet.entrySet()) {
      for (RID rid : entry.getValue()) {
        lockManager.unlockRow(txn, entry.getKey(), rid);
      }
    }

    for (String tableName : mergedTableLockSet) {
      lockManager.unlockTable(txn, tableName);
    }
  }

  public void makeInsertionLog(Transaction txn, RID rid, Tuple tuple) {
    LogRecord record =
        new LogRecord(txn.getTxn_id(), txn.getPrev_lsn(), LogRecordType.INSERT, rid, tuple);
    int lsn = logManager.appendLogRecord(record);
    txn.setPrev_lsn(lsn);
  }

  public void makeDeletionLog(Transaction txn, RID rid, Tuple tuple) {
    LogRecord record =
        new LogRecord(txn.getTxn_id(), txn.getPrev_lsn(), LogRecordType.APPLYDELETE, rid, tuple);
    int lsn = logManager.appendLogRecord(record);
    txn.setPrev_lsn(lsn);
  }

  public void commit(Transaction txn) {
    releaseAllLocks(txn);
    logManager.forceFlushLogs();
    txn.setState(TransactionState.COMMITTED);
  }

  public void abort(Transaction txn) {
    // TODO: revert all changes
    releaseAllLocks(txn);
    txn.setState(TransactionState.ABORTED);
  }
}
