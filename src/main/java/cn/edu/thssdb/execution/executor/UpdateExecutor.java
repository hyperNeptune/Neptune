package cn.edu.thssdb.execution.executor;

import cn.edu.thssdb.concurrency.LockManager;
import cn.edu.thssdb.concurrency.Transaction;
import cn.edu.thssdb.concurrency.TransactionManager;
import cn.edu.thssdb.execution.ExecContext;
import cn.edu.thssdb.parser.expression.Expression;
import cn.edu.thssdb.schema.Schema;
import cn.edu.thssdb.schema.TableInfo;
import cn.edu.thssdb.storage.Tuple;
import cn.edu.thssdb.type.Value;
import cn.edu.thssdb.utils.Pair;
import cn.edu.thssdb.utils.RID;

public class UpdateExecutor extends Executor {
  Executor child_;
  Schema schema_;
  Pair<String, Expression> updateValue_;
  TableInfo tableInfo_;
  private int updatedIdx_;

  public UpdateExecutor(
      ExecContext ctx, Executor plan, Pair<String, Expression> updateValue, TableInfo tab) {
    super(ctx);
    child_ = plan;
    updateValue_ = updateValue;
    tableInfo_ = tab;
    schema_ = tableInfo_.getSchema();
  }

  @Override
  public void init() throws Exception {
    child_.init();
    updatedIdx_ = schema_.getColumnOrder(updateValue_.left);
    Transaction txn = getCtx().getTransaction();
    LockManager lockManager = getCtx().getLockManager();
    lockManager.lockTable(txn, LockManager.LockMode.INTENTION_EXCLUSIVE, tableInfo_.getTableName());
  }

  @Override
  public boolean next(Tuple tuple, RID rid) throws Exception {
    if (!child_.next(tuple, rid)) {
      return false;
    }
    // copy values
    Value<?, ?>[] values = new Value[schema_.getColumns().length];
    for (int i = 0; i < schema_.getColumns().length; i++) {
      values[i] = tuple.getValue(schema_, i);
    }

    Tuple old_tuple = new Tuple(values, schema_);

    Transaction txn = getCtx().getTransaction();
    LockManager lockManager = getCtx().getLockManager();
    lockManager.lockRow(txn, LockManager.LockMode.EXCLUSIVE, tableInfo_.getTableName(), rid);
    TransactionManager transactionManager = getCtx().getTransactionManager();

    values[updatedIdx_] =
        schema_
            .getColumn(updatedIdx_)
            .getType()
            .castFrom(updateValue_.right.evaluation(tuple, schema_));
    tuple.copyAssign(new Tuple(values, schema_));
    tableInfo_.getTable().update(tuple, rid);

    transactionManager.makeUpdateLog(txn, rid, old_tuple, new Tuple(tuple));
    return true;
  }

  @Override
  public Schema getOutputSchema() {
    return schema_;
  }
}
