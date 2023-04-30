package cn.edu.thssdb.parser.expression;

import cn.edu.thssdb.type.Value;

public class UnaryExpression extends Expression {
  private Expression expression;
  private String op;

  public UnaryExpression(Expression expression, String op) {
    super(ExpressionType.UNARY);
    this.expression = expression;
    this.op = op;
  }

  public Expression getExpression() {
    return expression;
  }

  public String getOp() {
    return op;
  }

  @Override
  public Value<?, ?> getValue() {
    return null;
  }

  @Override
  public String toString() {
    return "UnaryExpression{" + "expression=" + expression + ", op='" + op + '\'' + '}';
  }
}
