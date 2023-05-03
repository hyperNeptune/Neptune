package cn.edu.thssdb.parser.expression;

import cn.edu.thssdb.schema.Schema;
import cn.edu.thssdb.storage.Tuple;
import cn.edu.thssdb.type.Value;

public class ColumnRefExpression extends Expression {
  // TODO: null table => current table, need to be filled in other modules
  private String table;
  private final String column;

  public ColumnRefExpression(String column) {
    super(ExpressionType.COLUMN_REF);
    this.column = column;
  }

  // with table
  public ColumnRefExpression(String table, String column) {
    super(ExpressionType.COLUMN_REF);
    this.table = table;
    this.column = column;
  }

  public String getTable() {
    return table;
  }

  // set
  public void setTable(String table) {
    this.table = table;
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
    return "ColumnRefExpression{" + "table=" + table + ", column=" + column + '}';
  }
}
