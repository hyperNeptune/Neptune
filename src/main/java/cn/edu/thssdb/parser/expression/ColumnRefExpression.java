package cn.edu.thssdb.parser.expression;

import cn.edu.thssdb.schema.Schema;
import cn.edu.thssdb.storage.Tuple;
import cn.edu.thssdb.type.Value;

public class ColumnRefExpression extends Expression {
  // TODO: null table => current table, need to be filled in other modules
  private final String column;

  public ColumnRefExpression(String column) {
    super(ExpressionType.COLUMN_REF);
    this.column = column;
  }

  // with table
  public ColumnRefExpression(String table, String column) {
    super(ExpressionType.COLUMN_REF);
    this.column = table + "." + column;
  }

  public String getColumn() {
    return column;
  }

  @Override
  public Value<?, ?> evaluation(Tuple tuple, Schema schema) {
    if (tuple == null || schema == null) {
      throw new RuntimeException("ColumnRefExpr: Tuple or schema is null!");
    }
    // find value
    return tuple.getValue(schema, schema.getColumnOrder(column));
  }

  @Override
  public String toString() {
    return "ColumnRefExpression{ column=" + column + '}';
  }
}
