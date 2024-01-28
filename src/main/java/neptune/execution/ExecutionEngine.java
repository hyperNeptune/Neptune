package neptune.execution;

import neptune.concurrency.TransactionManager;
import neptune.execution.executor.Executor;
import neptune.schema.Catalog;
import neptune.storage.Tuple;
import neptune.utils.RID;

import java.util.ArrayList;
import java.util.List;

public class ExecutionEngine {
  Catalog catalog_;
  TransactionManager txnm_;

  public ExecutionEngine(Catalog curDB, TransactionManager transactionManager) {
    catalog_ = curDB;
    txnm_ = transactionManager;
  }

  public boolean nullCatalog() {
    return catalog_ == null;
  }

  public void setCatalog(Catalog catalog) {
    catalog_ = catalog;
  }

  public List<Tuple> execute(Executor exec) throws Exception {
    List<Tuple> res = new ArrayList<>();
    exec.init();
    RID rid = new RID();
    Tuple tuple = new Tuple();
    while (exec.next(tuple, rid)) {
      res.add(new Tuple(tuple));
    }
    return res;
  }
}
