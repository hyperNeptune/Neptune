package cn.edu.thssdb.concurrency;

import cn.edu.thssdb.recovery.LogManager;

// Finance Minister in Louis XVI 's age
// Our transaction manager
public class TransactionManager {
  public TransactionManager(LogManager logManager) {}

  public Transaction begin() {
    return null;
  }

  public void commit(Transaction transaction) {}
}
