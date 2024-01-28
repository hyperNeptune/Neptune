package neptune.execution.executor;

import neptune.concurrency.LockManager;
import neptune.concurrency.Transaction;
import neptune.concurrency.TransactionManager;
import neptune.execution.ExecContext;
import neptune.schema.Schema;
import neptune.schema.TableInfo;
import neptune.storage.Tuple;
import neptune.utils.RID;

public class deleteExecutor extends Executor {
  private final Executor child_;
  private final TableInfo tableInfo_;

  public deleteExecutor(ExecContext ctx, Executor plan, TableInfo tableInfo) {
    super(ctx);
    child_ = plan;
    tableInfo_ = tableInfo;
  }

  @Override
  public void init() throws Exception {
    child_.init();
    Transaction txn = getCtx().getTransaction();
    LockManager lockManager = getCtx().getLockManager();
    lockManager.lockTable(txn, LockManager.LockMode.INTENTION_EXCLUSIVE, tableInfo_.getTableName());
  }

  @Override
  public boolean next(Tuple tuple, RID rid) throws Exception {
    if (!child_.next(tuple, rid)) {
      return false;
    }
    TransactionManager txnmgr = getCtx().getTransactionManager();
    LockManager lockManager = getCtx().getLockManager();
    Transaction txn = getCtx().getTransaction();
    txnmgr.makeDeletionLog(txn, rid, new Tuple(tableInfo_.getTable().getTuple(rid)));
    lockManager.lockRow(txn, LockManager.LockMode.EXCLUSIVE, tableInfo_.getTableName(), rid);

    tableInfo_.getTable().delete(rid);
    tableInfo_.getIndex().remove(rid, getCtx().getTransaction());
    return true;
  }

  @Override
  public Schema getOutputSchema() {
    return child_.getOutputSchema();
  }
}
