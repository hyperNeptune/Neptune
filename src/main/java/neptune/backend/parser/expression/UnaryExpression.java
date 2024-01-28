package neptune.backend.parser.expression;

import neptune.backend.schema.Schema;
import neptune.backend.storage.Tuple;
import neptune.backend.type.Value;

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
  public Value<?, ?> evaluation(Tuple tuple, Schema schema) {
    return null;
  }

  @Override
  public String toString() {
    return "UnaryExpression{" + "expression=" + expression + ", op='" + op + '\'' + '}';
  }
}
