package cn.edu.thssdb.parser.expression;

public class BinaryExpression extends Expression {
  private Expression left;
  private Expression right;
  private String op;

  public BinaryExpression(Expression left, Expression right, String op) {
    super(ExpressionType.BINARY);
    this.left = left;
    this.right = right;
    this.op = op;
  }

  public Expression getLeft() {
    return left;
  }

  public Expression getRight() {
    return right;
  }

  public String getOp() {
    return op;
  }

  @Override
  public String toString() {
    return "BinaryExpression{" + "left=" + left + ", right=" + right + ", op='" + op + '\'' + '}';
  }
}
