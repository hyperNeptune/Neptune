package cn.edu.thssdb.execution.executor;

import cn.edu.thssdb.execution.ExecContext;
import cn.edu.thssdb.schema.Schema;
import cn.edu.thssdb.schema.TableInfo;
import cn.edu.thssdb.storage.Tuple;
import cn.edu.thssdb.utils.RID;

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
    // empty for now
  }

  @Override
  public boolean next(Tuple tuple, RID rid) throws Exception {
    if (insertIndex_ >= tuples_.length) {
      return false;
    }
    Tuple t = tuples_[insertIndex_];
    tableInfo_.getTable().insert(t, rid);
    insertIndex_++;
    tuple.copyAssign(t);
    return true;
  }

  @Override
  public Schema getOutputSchema() {
    return tableInfo_.getSchema();
  }
}
