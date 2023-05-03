package cn.edu.thssdb.execution.executor;

import cn.edu.thssdb.execution.ExecContext;
import cn.edu.thssdb.parser.expression.Expression;
import cn.edu.thssdb.schema.Column;
import cn.edu.thssdb.schema.Schema;
import cn.edu.thssdb.storage.Tuple;
import cn.edu.thssdb.utils.RID;

public class nestedLoopJoinExecutor extends Executor {
  Executor left_;
  Executor right_;
  Expression on_;
  Schema joinOutSchema_;
  Tuple cur_tuple_;
  Tuple left_tuple_;
  RID left_rid_;
  RID right_rid_;
  Tuple right_tuple_;

  public nestedLoopJoinExecutor(Executor left, Executor right, Expression on, ExecContext ctx) {
    super(ctx);
    left_ = left;
    right_ = right;
    on_ = on;
    makeNewSchema();
  }

  private void makeNewSchema() {
    Column[] leftS = left_.getOutputSchema().getColumns();
    Column[] rightS = right_.getOutputSchema().getColumns();
    Column[] schemaC = new Column[leftS.length + rightS.length];
    // add all columns of left Schema
    int idx = 0;
    // we mustn't use a reference... must copy
    // because java objects are passed by reference. That's why Java is worse than cpp
    // In cpp we clearly distinguish pass-by-value pass-by-reference; copy/move etc.
    // whole range of essential operators are provided to empower the programmer with
    // full control on memory objects.
    // Whereas Java does not believe the programmer. Its memory model sucks. Writing
    // with Java objects is like struggling with the Phantom of the Opera.
    for (Column c : leftS) {
      schemaC[idx++] = new Column(c);
    }
    for (Column c : rightS) {
      schemaC[idx++] = new Column(c);
    }
    joinOutSchema_ = new Schema(schemaC);
  }

  @Override
  public void init() throws Exception {
    left_.init();
    right_.init();
    left_tuple_ = new Tuple();
    right_tuple_ = new Tuple();
    cur_tuple_ = new Tuple();
    left_rid_ = new RID();
    right_rid_ = new RID();
    // put first tuple on the desk.
    left_.next(left_tuple_, left_rid_);
  }

  @Override
  public boolean next(Tuple tuple, RID rid) throws Exception {
    // why java sucks #2
    // we need to allocate new objects by new, otherwise it will just be a null reference
    if (!right_.next(right_tuple_, right_rid_)) {
      // end or left should + 1
      if (!left_.next(left_tuple_, left_rid_)) {
        return false;
      }
      right_.init();
      right_.next(right_tuple_, right_rid_);
    }
    // join the two tuples

    return false;
  }

  @Override
  public Schema getOutputSchema() {
    if (joinOutSchema_ == null) {
      throw new RuntimeException("you haven't initialize nested loop join executor");
    }
    return joinOutSchema_;
  }
}
