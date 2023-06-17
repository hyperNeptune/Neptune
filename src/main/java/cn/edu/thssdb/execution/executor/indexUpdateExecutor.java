package cn.edu.thssdb.execution.executor;

import cn.edu.thssdb.concurrency.LockManager;
import cn.edu.thssdb.concurrency.Transaction;
import cn.edu.thssdb.execution.ExecContext;
import cn.edu.thssdb.parser.expression.Expression;
import cn.edu.thssdb.schema.Schema;
import cn.edu.thssdb.schema.TableInfo;
import cn.edu.thssdb.storage.Tuple;
import cn.edu.thssdb.type.Value;
import cn.edu.thssdb.utils.Pair;
import cn.edu.thssdb.utils.RID;

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
    values[updateIdx] = updateValue_.right.evaluation(null, null);
    tableInfo_.getTable().update(new Tuple(values, schema_), id);
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
