package cn.edu.thssdb.parser.statement;

import cn.edu.thssdb.parser.expression.Expression;
import cn.edu.thssdb.parser.tableBinder.TableBinder;

public class SelectStatement extends Statement {
  private final TableBinder from_;
  private final boolean isDistinct_;
  // expand * to all column names in planner
  // in binder we make a special column ref for it
  private final Expression[] selectList_;
  private final Expression where_;

  public SelectStatement(
      boolean isDistinct, Expression[] selectList, Expression where, TableBinder from) {
    super(StatementType.SELECT);
    this.isDistinct_ = isDistinct;
    this.selectList_ = selectList;
    this.where_ = where;
    this.from_ = from;
  }

  public TableBinder getFrom() {
    return from_;
  }

  public boolean isDistinct() {
    return isDistinct_;
  }

  public Expression[] getSelectList() {
    return selectList_;
  }

  public Expression getWhere() {
    return where_;
  }

  @Override
  public String toString() {
    return "statement: select";
  }
}
