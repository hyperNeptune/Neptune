package neptune.backend.execution;

import neptune.backend.buffer.BufferPoolManager;
import neptune.backend.concurrency.LockManager;
import neptune.backend.concurrency.Transaction;
import neptune.backend.concurrency.TransactionManager;
import neptune.backend.schema.Catalog;

// all information used by the executor
public class ExecContext {
  private final Transaction transaction_;
  private final Catalog catalog_;
  private final BufferPoolManager bpm_;
  private final LockManager lockManager_;
  private final TransactionManager transactionManager_;

  public ExecContext(
      Transaction transaction,
      Catalog catalog,
      BufferPoolManager bpm,
      LockManager lockManager,
      TransactionManager transactionManager) {
    transaction_ = transaction;
    catalog_ = catalog;
    bpm_ = bpm;
    lockManager_ = lockManager;
    transactionManager_ = transactionManager;
  }

  public Transaction getTransaction() {
    return transaction_;
  }

  public Catalog getCatalog() {
    return catalog_;
  }

  public BufferPoolManager getBufferPoolManager() {
    return bpm_;
  }

  public LockManager getLockManager() {
    return lockManager_;
  }

  public TransactionManager getTransactionManager() {
    return transactionManager_;
  }
}
