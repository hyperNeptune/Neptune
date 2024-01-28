package neptune.backend.execution.executor;

import neptune.backend.concurrency.LockManager;
import neptune.backend.concurrency.Transaction;
import neptune.backend.concurrency.TransactionManager;
import neptune.backend.execution.ExecContext;
import neptune.backend.schema.Schema;
import neptune.backend.schema.TableInfo;
import neptune.backend.storage.Tuple;
import neptune.backend.type.Value;
import neptune.common.RID;

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
