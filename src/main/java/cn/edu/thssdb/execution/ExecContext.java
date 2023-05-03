package cn.edu.thssdb.execution;

import cn.edu.thssdb.buffer.BufferPoolManager;
import cn.edu.thssdb.concurrency.Transaction;
import cn.edu.thssdb.schema.Catalog;

// all information used by the executor
public class ExecContext {
  private final Transaction transaction_;
  private final Catalog catalog_;
  private final BufferPoolManager bpm_;

  public ExecContext(Transaction transaction, Catalog catalog, BufferPoolManager bpm) {
    transaction_ = transaction;
    catalog_ = catalog;
    bpm_ = bpm;
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
}
