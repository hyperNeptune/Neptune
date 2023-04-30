package cn.edu.thssdb.parser.expression;

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

  public abstract Value<?, ?> getValue();

  @Override
  public String toString() {
    return "You should not expecting this";
  }
}
