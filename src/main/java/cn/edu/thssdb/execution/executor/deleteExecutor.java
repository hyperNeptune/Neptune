package cn.edu.thssdb.execution.executor;

import cn.edu.thssdb.concurrency.LockManager;
import cn.edu.thssdb.concurrency.Transaction;
import cn.edu.thssdb.concurrency.TransactionManager;
import cn.edu.thssdb.execution.ExecContext;
import cn.edu.thssdb.schema.Schema;
import cn.edu.thssdb.schema.TableInfo;
import cn.edu.thssdb.storage.Tuple;
import cn.edu.thssdb.utils.RID;

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
