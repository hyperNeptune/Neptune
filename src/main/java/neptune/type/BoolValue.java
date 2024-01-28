package neptune.type;

import java.nio.ByteBuffer;

public class BoolValue extends Value<BoolType, Boolean> {
  boolean value_;
  public static final BoolValue NULL_VALUE = new BoolValue();
  public static final BoolValue ALWAYS_TRUE = new BoolValue(true);
  public static final BoolValue ALWAYS_FALSE = new BoolValue(false);
  public static final BoolValue DEFAULT_VALUE = ALWAYS_FALSE;

  public BoolValue(boolean b) {
    super(BoolType.INSTANCE);
    value_ = b;
  }

  public BoolValue(Value<BoolType, Boolean> other) {
    super(other);
  }

  public BoolValue() {
    super(BoolType.INSTANCE);
    isNull_ = true;
    value_ = false;
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
    if (isNull_) {
      return "null";
    }
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
