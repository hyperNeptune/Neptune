package cn.edu.thssdb.parser.expression;

import cn.edu.thssdb.schema.Schema;
import cn.edu.thssdb.storage.Tuple;
import cn.edu.thssdb.type.Value;

public abstract class Expression {
  private final ExpressionType type;

  public Expression(ExpressionType type) {
    this.type = type;
  }

  public Expression() {
    this.type = ExpressionType.INVALID;
  }

  public ExpressionType getType() {
    return type;
  }

  // ColumnRefExpr must need tuple, schema. and others should have it
  // for passing it to bottom col ref expr.
  public abstract Value<?, ?> evaluation(Tuple tuple, Schema schema);

  @Override
  public String toString() {
    return "Expr: You should not expecting this";
  }
}
