package cn.edu.thssdb.parser.expression;

import cn.edu.thssdb.schema.Schema;
import cn.edu.thssdb.storage.Tuple;
import cn.edu.thssdb.type.BoolType;
import cn.edu.thssdb.type.BoolValue;
import cn.edu.thssdb.type.Value;

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
  public Value<?, ?> evaluation(Tuple tuple, Schema schema) {
    switch (op) {
      case "add":
        return left.evaluation(tuple, schema).add(right.evaluation(tuple, schema));
      case "sub":
        return left.evaluation(tuple, schema).sub(right.evaluation(tuple, schema));
      case "mul":
        return left.evaluation(tuple, schema).mul(right.evaluation(tuple, schema));
      case "div":
        return left.evaluation(tuple, schema).div(right.evaluation(tuple, schema));
      case "eq":
        return new BoolValue(
            left.evaluation(tuple, schema).equals(right.evaluation(tuple, schema)));
      case "ne":
        return new BoolValue(
            !left.evaluation(tuple, schema).equals(right.evaluation(tuple, schema)));
      case "gt":
        return new BoolValue(
            left.evaluation(tuple, schema).compareTo(right.evaluation(tuple, schema)) > 0);
      case "lt":
        return new BoolValue(
            left.evaluation(tuple, schema).compareTo(right.evaluation(tuple, schema)) < 0);
      case "ge":
        return new BoolValue(
            left.evaluation(tuple, schema).compareTo(right.evaluation(tuple, schema)) >= 0);
      case "le":
        return new BoolValue(
            left.evaluation(tuple, schema).compareTo(right.evaluation(tuple, schema)) <= 0);
      case "and":
        return new BoolValue(
            ((BoolValue) BoolType.INSTANCE.castFrom(left.evaluation(tuple, schema)))
                .and((BoolValue) BoolType.INSTANCE.castFrom(right.evaluation(tuple, schema))));
      case "or":
        return new BoolValue(
            ((BoolValue) BoolType.INSTANCE.castFrom(left.evaluation(tuple, schema)))
                .or((BoolValue) BoolType.INSTANCE.castFrom(right.evaluation(tuple, schema))));
      default:
        throw new RuntimeException("Unsupported operator: " + op);
    }
  }

  @Override
  public String toString() {
    return "BinaryExpression{" + "left=" + left + ", right=" + right + ", op='" + op + '\'' + '}';
  }
}
