package neptune.execution.executor;

import neptune.concurrency.LockManager;
import neptune.concurrency.Transaction;
import neptune.concurrency.TransactionManager;
import neptune.execution.ExecContext;
import neptune.schema.Schema;
import neptune.schema.TableInfo;
import neptune.storage.Tuple;
import neptune.type.Value;
import neptune.utils.RID;

public class InsertExecutor extends Executor {
  ExecContext ctx_;
  TableInfo tableInfo_;
  Tuple[] tuples_;
  int insertIndex_;

  public InsertExecutor(ExecContext ctx, TableInfo tableInfo, Tuple[] tuples) {
    super(ctx);
    ctx_ = ctx;
    tableInfo_ = tableInfo;
    tuples_ = tuples;
  }

  @Override
  public void init() throws Exception {
    insertIndex_ = 0;
    LockManager lockManager = getCtx().getLockManager();
    Transaction txn = getCtx().getTransaction();
    lockManager.lockTable(txn, LockManager.LockMode.INTENTION_EXCLUSIVE, tableInfo_.getTableName());
    // empty for now
  }

  @Override
  public boolean next(Tuple tuple, RID rid) throws Exception {
    if (insertIndex_ >= tuples_.length) {
      return false;
    }
    Tuple t = tuples_[insertIndex_];
    // 1.logging
    // 2.make it atomic
    TransactionManager txnmgr = getCtx().getTransactionManager();
    LockManager lockManager = getCtx().getLockManager();
    Transaction txn = getCtx().getTransaction();

    tableInfo_.getTable().insert(t, rid);
    lockManager.lockRow(txn, LockManager.LockMode.EXCLUSIVE, tableInfo_.getTableName(), rid);
    txnmgr.makeInsertionLog(txn, rid, t);

    Value<?, ?> pkValue = t.getValue(tableInfo_.getSchema(), tableInfo_.getSchema().getPkIndex());
    if (!tableInfo_.getIndex().insert(pkValue, rid, getCtx().getTransaction())) {
      tableInfo_.getTable().delete(rid);
      throw new Exception("insert failed, unique key constraint violated");
    }
    insertIndex_++;
    tuple.copyAssign(t);
    return true;
  }

  @Override
  public Schema getOutputSchema() {
    return tableInfo_.getSchema();
  }
}
