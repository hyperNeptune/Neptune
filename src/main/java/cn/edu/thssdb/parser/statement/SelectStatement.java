package cn.edu.thssdb.parser.statement;

import cn.edu.thssdb.parser.expression.BinaryExpression;
import cn.edu.thssdb.parser.expression.ColumnRefExpression;
import cn.edu.thssdb.parser.expression.Expression;
import cn.edu.thssdb.parser.expression.ExpressionType;
import cn.edu.thssdb.parser.tableBinder.RegularTableBinder;
import cn.edu.thssdb.parser.tableBinder.TableBinder;
import cn.edu.thssdb.parser.tableBinder.TableBinderType;
import cn.edu.thssdb.schema.Column;
import cn.edu.thssdb.schema.TableInfo;

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

  public boolean useIndex() {
    if (from_.getType() != TableBinderType.REGULAR) {
      return false;
    }
    RegularTableBinder regularTableBinder = (RegularTableBinder) from_;
    TableInfo tableInfo = regularTableBinder.getTableInfo();
    Column pk = tableInfo.getPKColumn();
    if (pk == null) {
      return false;
    }
    // check where clause. we support binary expression, like PK op const
    if (where_ == null) {
      // use Index has no advantage
      return false;
    }
    if (where_.getType() != ExpressionType.BINARY) {
      return false;
    }
    BinaryExpression binaryExpression = (BinaryExpression) where_;
    Expression left = binaryExpression.getLeft();
    Expression right = binaryExpression.getRight();
    if (left.getType() != ExpressionType.COLUMN_REF) {
      return false;
    }
    if (right.getType() != ExpressionType.CONSTANT) {
      return false;
    }
    return ((ColumnRefExpression) left).getColumn().equals(pk.getName());
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
