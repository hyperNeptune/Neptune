package cn.edu.thssdb.parser.tableBinder;

import cn.edu.thssdb.parser.expression.BinaryExpression;
import cn.edu.thssdb.parser.expression.ColumnRefExpression;
import cn.edu.thssdb.parser.expression.Expression;
import cn.edu.thssdb.parser.expression.ExpressionType;

public class JoinTableBinder extends TableBinder {
  private final TableBinder left;
  private final TableBinder right;
  private final Expression on;

  public JoinTableBinder(TableBinder left, TableBinder right, Expression on) {
    super(TableBinderType.JOIN);
    this.left = left;
    this.right = right;
    this.on = on;
  }

  @Override
  public String toString() {
    return "JoinTableBinder{}";
  }

  public TableBinder getLeft() {
    return left;
  }

  public TableBinder getRight() {
    return right;
  }

  public Expression getOn() {
    return on;
  }

  public boolean joinOnPK() {
    if (left.getType() != TableBinderType.REGULAR || right.getType() != TableBinderType.REGULAR) {
      return false;
    }
    RegularTableBinder leftRegular = (RegularTableBinder) left;
    RegularTableBinder rightRegular = (RegularTableBinder) right;
    if (on.getType() != ExpressionType.BINARY) {
      return false;
    }
    BinaryExpression binaryExpression = (BinaryExpression) on;
    Expression left = binaryExpression.getLeft();
    Expression right = binaryExpression.getRight();
    if (left.getType() != ExpressionType.COLUMN_REF
        || right.getType() != ExpressionType.COLUMN_REF) {
      return false;
    }
    String leftColumnName = ((ColumnRefExpression) left).getColumn();
    String rightColumnName = ((ColumnRefExpression) right).getColumn();
    return leftRegular.getTableInfo().getPKColumn().getFullName().equals(leftColumnName)
        && rightRegular.getTableInfo().getPKColumn().getFullName().equals(rightColumnName);
  }
}
