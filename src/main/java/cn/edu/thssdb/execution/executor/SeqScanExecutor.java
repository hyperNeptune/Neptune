package cn.edu.thssdb.execution.executor;

import cn.edu.thssdb.execution.ExecContext;
import cn.edu.thssdb.schema.Schema;
import cn.edu.thssdb.schema.TableInfo;
import cn.edu.thssdb.storage.Tuple;
import cn.edu.thssdb.utils.Pair;
import cn.edu.thssdb.utils.RID;

import java.util.Iterator;

public class SeqScanExecutor extends Executor {
  Schema schema_;
  TableInfo tableInfo_;
  Tuple tuple_;
  RID rid_;
  Iterator<Pair<Tuple, RID>> iterator_;

  public SeqScanExecutor(TableInfo tableInfo, ExecContext ctx) {
    super(ctx);
    this.tableInfo_ = tableInfo;
    this.schema_ = tableInfo.getSchema();
  }

  @Override
  public void init() throws Exception {
    iterator_ = tableInfo_.getTable().iterator();
  }

  @Override
  public boolean next(Tuple tuple, RID rid) {
    if (iterator_.hasNext()) {
      Pair<Tuple, RID> pair = iterator_.next();
      tuple_ = pair.left;
      rid_ = pair.right;
      tuple.copyAssign(tuple_);
      rid.assign(rid_);
      return true;
    }
    return false;
  }

  @Override
  public Schema getOutputSchema() {
    return schema_;
  }
}
