package neptune.backend.parser.statement;

import neptune.backend.parser.expression.*;
import neptune.backend.parser.tableBinder.JoinTableBinder;
import neptune.backend.parser.tableBinder.RegularTableBinder;
import neptune.backend.parser.tableBinder.TableBinder;
import neptune.backend.parser.tableBinder.TableBinderType;
import neptune.backend.schema.Column;
import neptune.backend.schema.TableInfo;
import neptune.backend.type.Value;

import java.util.Objects;

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

  public Value<?, ?> useIndexJoin() {
    // Join on pk
    if (from_.getType() != TableBinderType.JOIN) {
      return null;
    }
    JoinTableBinder joinTableBinder = (JoinTableBinder) from_;
    if (!joinTableBinder.joinOnPK()) {
      return null;
    }

    // where like 'pk = const'
    if (where_ == null) {
      return null;
    }
    if (where_.getType() != ExpressionType.BINARY) {
      return null;
    }
    BinaryExpression binaryExpression = (BinaryExpression) where_;
    Expression left = binaryExpression.getLeft();
    Expression right = binaryExpression.getRight();
    if (left.getType() != ExpressionType.COLUMN_REF) {
      return null;
    }
    if (right.getType() != ExpressionType.CONSTANT) {
      return null;
    }
    if (!Objects.equals(binaryExpression.getOp(), "eq")) {
      return null;
    }
    ColumnRefExpression columnRefExpression = (ColumnRefExpression) left;
    String columnName = columnRefExpression.getColumn();
    TableBinder leftTableBinder = joinTableBinder.getLeft();
    TableBinder rightTableBinder = joinTableBinder.getRight();
    if (leftTableBinder.getType() != TableBinderType.REGULAR) {
      return null;
    }
    if (rightTableBinder.getType() != TableBinderType.REGULAR) {
      return null;
    }
    RegularTableBinder leftRegularTableBinder = (RegularTableBinder) leftTableBinder;
    RegularTableBinder rightRegularTableBinder = (RegularTableBinder) rightTableBinder;
    TableInfo leftTableInfo = leftRegularTableBinder.getTableInfo();
    TableInfo rightTableInfo = rightRegularTableBinder.getTableInfo();
    if (leftTableInfo.getSchema().getPkColumn().getFullName().equals(columnName)
        || rightTableInfo.getSchema().getPkColumn().getFullName().equals(columnName)) {
      ConstantExpression constantExpression = (ConstantExpression) right;
      return constantExpression.evaluation(null, null);
    }
    return null;
  }
}
