package neptune.backend.parser.expression;

import neptune.backend.schema.Schema;
import neptune.backend.storage.Tuple;
import neptune.backend.type.BoolValue;
import neptune.backend.type.Type;
import neptune.backend.type.Value;

// TODO: how to spawn null value from constant expression?
// maybe **refactor** null value. null can be a separated singleton class.
public class ConstantExpression extends Expression {
  public static final ConstantExpression ALWAYS_TRUE_EXPRESSION =
      new ConstantExpression(BoolValue.ALWAYS_TRUE);
  public static final ConstantExpression ALWAYS_FALSE_EXPRESSION =
      new ConstantExpression(BoolValue.ALWAYS_FALSE);

  private final Value<? extends Type, ?> value;

  public ConstantExpression(Value<? extends Type, ?> value) {
    super(ExpressionType.CONSTANT);
    this.value = value;
  }

  public Value<? extends Type, ?> evaluation(Tuple tuple, Schema schema) {
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
