package cn.edu.thssdb.parser.expression;

public class Expression {
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

  @Override
  public String toString() {
    return "You should not expecting this";
  }
}
