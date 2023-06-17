package cn.edu.thssdb.execution.executor;

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
    values[updatedIdx_] =
        schema_
            .getColumn(updatedIdx_)
            .getType()
            .castFrom(updateValue_.right.evaluation(tuple, schema_));
    tuple.copyAssign(new Tuple(values, schema_));
    tableInfo_.getTable().update(tuple, rid);
    return true;
  }

  @Override
  public Schema getOutputSchema() {
    return schema_;
  }
}
