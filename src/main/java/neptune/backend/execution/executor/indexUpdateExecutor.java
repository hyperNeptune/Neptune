package neptune.backend.execution.executor;

import neptune.backend.concurrency.LockManager;
import neptune.backend.concurrency.Transaction;
import neptune.backend.concurrency.TransactionManager;
import neptune.backend.execution.ExecContext;
import neptune.backend.parser.expression.Expression;
import neptune.backend.schema.Schema;
import neptune.backend.schema.TableInfo;
import neptune.backend.storage.Tuple;
import neptune.backend.type.Value;
import neptune.common.Pair;
import neptune.common.RID;

public class indexUpdateExecutor extends Executor {
  Schema schema_;
  Pair<String, Expression> updateValue_;
  TableInfo tableInfo_;
  Value<?, ?> updatePK_;
  int updateIdx;

  public indexUpdateExecutor(
      ExecContext ctx, Pair<String, Expression> updateValue, TableInfo tab, Value<?, ?> updatePK) {
    super(ctx);
    updateValue_ = updateValue;
    tableInfo_ = tab;
    schema_ = tableInfo_.getSchema();
    updateIdx = schema_.getColumnOrder(updateValue_.left);
    updatePK_ = schema_.getPkColumn().getType().castFrom(updatePK);

    Transaction txn = getCtx().getTransaction();
    LockManager lockManager = getCtx().getLockManager();
    lockManager.lockTable(txn, LockManager.LockMode.INTENTION_EXCLUSIVE, tableInfo_.getTableName());
  }

  @Override
  public void init() throws Exception {
    RID id = tableInfo_.getIndex().getValue(updatePK_, getCtx().getTransaction());
    if (id == null) {
      return;
    }
    Tuple updateTuple = tableInfo_.getTable().getTuple(id);
    Value<?, ?>[] values = new Value[schema_.getColumns().length];
    for (int i = 0; i < schema_.getColumns().length; i++) {
      values[i] = updateTuple.getValue(schema_, i);
    }
    values[updateIdx] =
        schema_.getColumn(updateIdx).getType().castFrom(updateValue_.right.evaluation(null, null));

    Transaction txn = getCtx().getTransaction();
    LockManager lockManager = getCtx().getLockManager();
    lockManager.lockRow(txn, LockManager.LockMode.EXCLUSIVE, tableInfo_.getTableName(), id);

    tableInfo_.getTable().update(new Tuple(values, schema_), id);

    TransactionManager transactionManager = getCtx().getTransactionManager();
    transactionManager.makeUpdateLog(txn, id, new Tuple(updateTuple), new Tuple(values, schema_));
  }

  @Override
  public boolean next(Tuple tuple, RID rid) throws Exception {
    return false;
  }

  @Override
  public Schema getOutputSchema() {
    return schema_;
  }
}
