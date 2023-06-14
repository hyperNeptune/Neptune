package cn.edu.thssdb.execution.executor;

import cn.edu.thssdb.execution.ExecContext;
import cn.edu.thssdb.parser.expression.Expression;
import cn.edu.thssdb.parser.tableBinder.TableBinder;
import cn.edu.thssdb.schema.Schema;
import cn.edu.thssdb.storage.Tuple;
import cn.edu.thssdb.utils.RID;

// TODO: xrf indexScanExecutor
public class indexScanExecutor extends Executor {
  TableBinder from_;
  Expression where_;

  public indexScanExecutor(ExecContext ctx, TableBinder from, Expression where) {
    super(ctx);
    from_ = from;
    where_ = where;
  }

  @Override
  public void init() throws Exception {}

  @Override
  public boolean next(Tuple tuple, RID rid) throws Exception {
    return false;
  }

  @Override
  public Schema getOutputSchema() {
    return null;
  }

  @Override
  public String toString() {
    return "indexScanExecutor";
  }
}
