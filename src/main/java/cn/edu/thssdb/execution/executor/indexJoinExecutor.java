package cn.edu.thssdb.execution.executor;

import cn.edu.thssdb.execution.ExecContext;
import cn.edu.thssdb.schema.Column;
import cn.edu.thssdb.schema.Schema;
import cn.edu.thssdb.schema.TableInfo;
import cn.edu.thssdb.storage.Tuple;
import cn.edu.thssdb.type.Value;
import cn.edu.thssdb.utils.RID;

public class indexJoinExecutor extends Executor {
  Value<?, ?> pkValue_;
  TableInfo leftTable;
  TableInfo rightTable;
  Schema joinOutSchema_;
  Tuple cur_tuple_;
  Tuple left_tuple_;
  RID left_rid_;
  RID right_rid_;
  Tuple right_tuple_;
  boolean drain = false;

  public indexJoinExecutor(TableInfo left, TableInfo right, Value<?, ?> pkValue, ExecContext ctx) {
    super(ctx);
    leftTable = left;
    rightTable = right;
    pkValue_ = pkValue;
    makeNewSchema();
  }

  private void makeNewSchema() {
    Column[] leftS = leftTable.getSchema().getColumns();
    Column[] rightS = rightTable.getSchema().getColumns();
    Column[] schemaC = new Column[leftS.length + rightS.length];
    // add all columns of left Schema
    int idx = 0;
    for (Column c : leftS) {
      if (c.getFullName() != null) {
        schemaC[idx++] = Column.ColumnRenameFactory(c, c.getFullName());
      } else {
        schemaC[idx++] = new Column(c);
      }
    }
    for (Column c : rightS) {
      if (c.getFullName() != null) {
        schemaC[idx++] = Column.ColumnRenameFactory(c, c.getFullName());
      } else {
        schemaC[idx++] = new Column(c);
      }
    }
    joinOutSchema_ = new Schema(schemaC);
  }

  @Override
  public void init() throws Exception {
    left_rid_ = leftTable.getIndex().getValue(pkValue_, getCtx().getTransaction());
    if (left_rid_ == null) {
      drain = true;
      return;
    }
    left_tuple_ = leftTable.getTable().getTuple(left_rid_);
    right_rid_ = rightTable.getIndex().getValue(pkValue_, getCtx().getTransaction());
    if (right_rid_ == null) {
      drain = true;
      return;
    }
    right_tuple_ = rightTable.getTable().getTuple(right_rid_);
    // concat them
    Value<?, ?>[] valueJoin = new Value[joinOutSchema_.getColNum()];
    int idx_join;
    Schema leftSchema = leftTable.getSchema();
    Schema rightSchema = rightTable.getSchema();
    for (idx_join = 0; idx_join < leftSchema.getColNum(); idx_join++) {
      valueJoin[idx_join] = left_tuple_.getValue(leftSchema, idx_join);
    }
    for (int idx_right = 0; idx_right < rightSchema.getColNum(); idx_right++) {
      valueJoin[idx_join++] = right_tuple_.getValue(rightSchema, idx_right);
    }
    cur_tuple_ = new Tuple(valueJoin, joinOutSchema_);
  }

  // for this, RID is meaningless, because we are generating non-exist tuples.
  @Override
  public boolean next(Tuple tuple, RID rid) throws Exception {
    if (drain) {
      return false;
    }
    tuple.copyAssign(cur_tuple_);
    return drain = true;
  }

  @Override
  public Schema getOutputSchema() {
    if (joinOutSchema_ == null) {
      throw new RuntimeException("you haven't initialize index loop join executor");
    }
    return joinOutSchema_;
  }
}
