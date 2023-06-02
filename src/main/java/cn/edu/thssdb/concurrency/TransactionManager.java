package cn.edu.thssdb.concurrency;

import cn.edu.thssdb.recovery.LogManager;
import cn.edu.thssdb.recovery.LogRecord;
import cn.edu.thssdb.recovery.LogRecordType;

import java.util.HashMap;
import java.util.concurrent.locks.ReentrantLock;

public class TransactionManager {
  private LockManager lockManager;
  private final LogManager logManager;

  private final HashMap<Integer, Transaction> txn_map = new HashMap<>();
  private final ReentrantLock txn_map_latch = new ReentrantLock();

  private int currentTxnID;
  private final ReentrantLock txn_id_latch = new ReentrantLock();

  public TransactionManager(LogManager logManager) {
    this.logManager = logManager;
    this.currentTxnID = 0;
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

  public void commit(Transaction txn) {}

  public void abort(Transaction txn) {}
}
