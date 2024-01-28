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

public class indexDeleteExecutor extends Executor {
  private final Value<?, ?> delValue_;
  private final TableInfo tableInfo_;

  public indexDeleteExecutor(ExecContext ctx, Value<?, ?> delValue, TableInfo tableInfo) {
    super(ctx);
    delValue_ = delValue;
    tableInfo_ = tableInfo;
  }

  @Override
  public void init() throws Exception {
    RID rid = tableInfo_.getIndex().getValue(delValue_, getCtx().getTransaction());
    if (rid == null) {
      return;
    }
    Transaction txn = getCtx().getTransaction();
    LockManager lockManager = getCtx().getLockManager();

    TransactionManager transactionManager = getCtx().getTransactionManager();
    transactionManager.makeDeletionLog(txn, rid, new Tuple(tableInfo_.getTable().getTuple(rid)));

    lockManager.lockTable(txn, LockManager.LockMode.INTENTION_EXCLUSIVE, tableInfo_.getTableName());
    lockManager.lockRow(txn, LockManager.LockMode.EXCLUSIVE, tableInfo_.getTableName(), rid);

    tableInfo_.getTable().delete(rid);
    tableInfo_.getIndex().remove(rid, getCtx().getTransaction());
  }

  @Override
  public boolean next(Tuple tuple, RID rid) throws Exception {
    return false;
  }

  @Override
  public Schema getOutputSchema() {
    return tableInfo_.getSchema();
  }
}
