package cn.edu.thssdb.execution;

import cn.edu.thssdb.concurrency.TransactionManager;
import cn.edu.thssdb.execution.executor.Executor;
import cn.edu.thssdb.schema.Catalog;
import cn.edu.thssdb.storage.Tuple;

import java.util.List;

public class ExecutionEngine {
  Catalog catalog_;

  public ExecutionEngine(Catalog curDB, TransactionManager transactionManager) {}

  public boolean nullCatalog() {
    return catalog_ == null;
  }

  public void setCatalog(Catalog catalog) {
    catalog_ = catalog;
  }

  public List<Tuple> execute(Executor exec, ExecContext ctx) {
    return null;
  }
}
