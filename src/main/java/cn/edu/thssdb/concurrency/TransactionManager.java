package cn.edu.thssdb.concurrency;

import cn.edu.thssdb.recovery.LogManager;

public class TransactionManager {
  private LockManager lockManager;
  private LogManager logManager;
  public TransactionManager(LogManager logManager) {}

  public Transaction begin(IsolationLevel isolationLevel) {
    return null;
  }

  public void commit(Transaction transaction) {}


}
