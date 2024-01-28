package neptune.backend.execution;

import neptune.backend.concurrency.TransactionManager;
import neptune.backend.execution.executor.Executor;
import neptune.backend.schema.Catalog;
import neptune.backend.storage.Tuple;
import neptune.common.RID;

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
