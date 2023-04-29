package cn.edu.thssdb.execution;

import cn.edu.thssdb.concurrency.TransactionManager;
import cn.edu.thssdb.schema.Catalog;

public class ExecutionEngine {
  Catalog catalog_;

  public ExecutionEngine(Catalog curDB, TransactionManager transactionManager) {}

  public boolean nullCatalog() {
    return catalog_ == null;
  }

  public void setCatalog(Catalog catalog) {
    catalog_ = catalog;
  }
}
