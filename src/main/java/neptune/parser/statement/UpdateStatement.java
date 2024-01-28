package neptune.parser.statement;

import neptune.parser.expression.*;
import neptune.schema.TableInfo;
import neptune.type.Value;
import neptune.utils.Pair;

public class UpdateStatement extends Statement {
  TableInfo tableInfo_;
  Pair<String, Expression> updateValue_;
  Expression where_;

  public UpdateStatement(
      TableInfo tableInfo, Pair<String, Expression> updateValue, Expression where) {
    super(StatementType.UPDATE);
    this.tableInfo_ = tableInfo;
    this.updateValue_ = updateValue;
    this.where_ = where;
  }

  public TableInfo getTable() {
    return tableInfo_;
  }

  public Pair<String, Expression> getUpdateValue() {
    return updateValue_;
  }

  public Expression getWhere() {
    return where_;
  }

  @Override
  public String toString() {
    return "statement: update";
  }

  public Value<?, ?> useIndex() {
    return findKey(where_, tableInfo_);
  }

  static Value<?, ?> findKey(Expression where, TableInfo tableInfo) {
    if (where.getType() != ExpressionType.BINARY) {
      return null;
    }
    BinaryExpression binaryExpression = (BinaryExpression) where;
    if (binaryExpression.getLeft().getType() != ExpressionType.COLUMN_REF) {
      return null;
    }
    if (binaryExpression.getRight().getType() != ExpressionType.CONSTANT) {
      return null;
    }
    ColumnRefExpression columnRefExpression = (ColumnRefExpression) binaryExpression.getLeft();
    ConstantExpression constantExpression = (ConstantExpression) binaryExpression.getRight();
    if (columnRefExpression.getColumn().equals(tableInfo.getPKColumn().getName())) {
      return constantExpression.evaluation(null, null);
    }
    return null;
  }
}
