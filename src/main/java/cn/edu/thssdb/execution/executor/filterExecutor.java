package cn.edu.thssdb.execution.executor;

import cn.edu.thssdb.execution.ExecContext;
import cn.edu.thssdb.parser.expression.Expression;
import cn.edu.thssdb.schema.Schema;
import cn.edu.thssdb.storage.Tuple;
import cn.edu.thssdb.type.BoolType;
import cn.edu.thssdb.type.BoolValue;
import cn.edu.thssdb.utils.RID;

public class filterExecutor extends Executor {
  Executor child_;
  Tuple cur_tuple_;
  Expression where_;

  public filterExecutor(Executor plan, Expression where, ExecContext ctx) {
    super(ctx);
    child_ = plan;
    where_ = where;
  }

  @Override
  public void init() throws Exception {
    cur_tuple_ = new Tuple();
    child_.init();
  }

  @Override
  public boolean next(Tuple tuple, RID rid) throws Exception {
    do {
      if (!child_.next(cur_tuple_, rid)) {
        return false;
      }
    } while (!((BoolValue)
            BoolType.INSTANCE.castFrom(where_.evaluation(cur_tuple_, child_.getOutputSchema())))
        .getValue());
    tuple.copyAssign(cur_tuple_);
    return true;
  }

  @Override
  public Schema getOutputSchema() {
    return child_.getOutputSchema();
  }
}
