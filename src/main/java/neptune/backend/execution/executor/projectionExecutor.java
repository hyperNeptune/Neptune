package neptune.backend.execution.executor;

import neptune.backend.execution.ExecContext;
import neptune.backend.parser.expression.ColumnRefExpression;
import neptune.backend.parser.expression.Expression;
import neptune.backend.schema.Column;
import neptune.backend.schema.Schema;
import neptune.backend.storage.Tuple;
import neptune.backend.type.Type;
import neptune.backend.type.Value;
import neptune.common.RID;

public class projectionExecutor extends Executor {
  Executor child_;
  Expression[] select_;
  Schema outputSchema_;
  Tuple cur_tuple_;
  boolean calculator_extracted = false;
  int[] child_father_mapping; // output schema index -> input schema index

  public projectionExecutor(ExecContext ctx, Executor child, Expression[] select) {
    super(ctx);
    child_ = child;
    select_ = select;
    makeProjectionSchema();
  }

  private void makeProjectionSchema() {
    // calculator
    if (child_ == null) {
      Column[] columns = new Column[select_.length];
      Value[] values = new Value[select_.length];
      int idx = 0;
      for (Expression e : select_) {
        Value V = e.evaluation(null, null);
        Type T = V.getType();
        columns[idx] = new Column(e.toString(), T, (byte) 0, (byte) 0, V.getSize(), 0);
        values[idx] = V;
        idx++;
      }
      outputSchema_ = new Schema(columns);
      cur_tuple_ = new Tuple(values, outputSchema_);
      return;
    }

    Schema childS = child_.getOutputSchema();

    // select *?
    for (Expression e : select_) {
      ColumnRefExpression colRef = (ColumnRefExpression) e;
      if (colRef.getColumn() == null) {
        throw new RuntimeException("projectionExecutor: column name is null!");
      }
      if (colRef.getColumn().equals("*")) {
        outputSchema_ = new Schema(childS.getColumns());
        // identical mapping
        child_father_mapping = new int[childS.getColNum()];
        for (int i = 0; i < childS.getColNum(); i++) {
          child_father_mapping[i] = i;
        }
        return;
      }
    }

    // some columns
    Column[] columns = new Column[select_.length];
    child_father_mapping = new int[select_.length];
    int idx = 0;
    for (Expression e : select_) {
      ColumnRefExpression cre = (ColumnRefExpression) e;
      if (cre == null) {
        throw new RuntimeException("projectionExecutor: column name is invalid!");
      }
      String colName = cre.getColumn();
      int colIdx = childS.getColumnOrder(colName);
      // copy!! don't use the same column
      columns[idx] = new Column(childS.getColumns()[colIdx]);
      child_father_mapping[idx] = colIdx;
      idx++;
    }
    outputSchema_ = new Schema(columns);
  }

  @Override
  public void init() throws Exception {
    if (child_ != null) {
      child_.init();
    }
  }

  @Override
  public boolean next(Tuple tuple, RID rid) throws Exception {
    if (calculator_extracted) {
      return false;
    }
    if (child_ == null) {
      if (cur_tuple_ == null) {
        throw new RuntimeException("projectionExecutor: cur_tuple_ is null! maybe did not init?");
      }
      tuple.copyAssign(cur_tuple_);
      calculator_extracted = true;
      return true;
    }

    // projection for regular rows
    Tuple childTuple = new Tuple();
    if (!child_.next(childTuple, rid)) {
      return false;
    }
    Value[] values = new Value[outputSchema_.getColNum()];
    for (int i = 0; i < outputSchema_.getColNum(); i++) {
      values[i] = childTuple.getValue(child_.getOutputSchema(), child_father_mapping[i]);
    }
    cur_tuple_ = new Tuple(values, outputSchema_);
    tuple.copyAssign(cur_tuple_);
    return true;
  }

  @Override
  public Schema getOutputSchema() {
    return outputSchema_;
  }
}
