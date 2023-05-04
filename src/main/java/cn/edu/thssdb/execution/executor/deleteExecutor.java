package cn.edu.thssdb.execution.executor;

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
  }

  @Override
  public boolean next(Tuple tuple, RID rid) throws Exception {
    if (!child_.next(tuple, rid)) {
      return false;
    }
    tableInfo_.getTable().delete(rid);
    return true;
  }

  @Override
  public Schema getOutputSchema() {
    return child_.getOutputSchema();
  }
}
