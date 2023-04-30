package cn.edu.thssdb.parser.expression;

import cn.edu.thssdb.type.Type;
import cn.edu.thssdb.type.Value;

public class ConstantExpression extends Expression {
  private Value<? extends Type, ?> value;

  public ConstantExpression(Value<? extends Type, ?> value) {
    super(ExpressionType.CONSTANT);
    this.value = value;
  }

  public Value<? extends Type, ?> getValue() {
    return value;
  }

  public boolean isNull() {
    return value == null;
  }

  @Override
  public String toString() {
    return "ConstantExpression{" + "value=" + value + '}';
  }
}
