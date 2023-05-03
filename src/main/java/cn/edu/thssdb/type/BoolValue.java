package cn.edu.thssdb.type;

import java.nio.ByteBuffer;

public class BoolValue extends Value<BoolType, Boolean> {
  boolean value_;
  public static final BoolValue NULL_VALUE = new BoolValue(true);

  public BoolValue(boolean b) {
    super(BoolType.INSTANCE);
    value_ = b;
  }

  public BoolValue(Value<BoolType, Boolean> other) {
    super(other);
  }

  @Override
  public int getSize() {
    return 1;
  }

  @Override
  public void serialize(ByteBuffer buffer, int offset) {
    if (value_) {
      buffer.put(offset, (byte) 1);
    } else {
      buffer.put(offset, (byte) 0);
    }
  }

  @Override
  public String toString() {
    if (value_) {
      return "true";
    }
    return "false";
  }

  @Override
  public Boolean getValue() {
    return value_;
  }

  // and, or, not operations are solely supported in bool type
  // if needed in other types, cast first.
  public Value<BoolType, Boolean> and(Value<BoolType, Boolean> other) {
    return new BoolValue(value_ && other.getValue());
  }

  public Value<BoolType, Boolean> or(Value<BoolType, Boolean> other) {
    return new BoolValue(value_ || other.getValue());
  }

  public Value<BoolType, Boolean> not() {
    return new BoolValue(!value_);
  }
}
