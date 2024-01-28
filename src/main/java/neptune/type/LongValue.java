package neptune.type;

import neptune.utils.Global;

import java.nio.ByteBuffer;

public class LongValue extends Value<LongType, Long> {

  long value_;

  public static final LongValue NULL_VALUE = new LongValue();

  // constructor
  public LongValue(long value) {
    super(LongType.INSTANCE);
    value_ = value;
  }

  public LongValue() {
    super(LongType.INSTANCE);
    isNull_ = true;
    value_ = 0;
  }

  @Override
  public int getSize() {
    return Global.LONG_SIZE;
  }

  @Override
  public void serialize(ByteBuffer buffer, int offset) {
    buffer.putLong(offset, value_);
  }

  @Override
  public String toString() {
    if (isNull_) {
      return "null";
    }
    return Long.toString(value_);
  }

  @Override
  public Long getValue() {
    return value_;
  }

  @Override
  protected Value<? extends Type, ?> addImpl(Value<? extends Type, ?> other, boolean reverse) {
    if (other.getType() == LongType.INSTANCE) {
      return new LongValue(((LongValue) other).getValue() + value_);
    }
    return super.addImpl(other, reverse);
  }

  @Override
  protected Value<? extends Type, ?> subImpl(Value<? extends Type, ?> other, boolean reverse) {
    if (other.getType() == LongType.INSTANCE) {
      if (reverse) {
        return new LongValue(((LongValue) other).getValue() - value_);
      } else {
        return new LongValue(value_ - ((LongValue) other).getValue());
      }
    }
    return super.subImpl(other, reverse);
  }

  @Override
  protected Value<? extends Type, ?> mulImpl(Value<? extends Type, ?> other, boolean reverse) {
    if (other.getType() == LongType.INSTANCE) {
      return new LongValue(((LongValue) other).getValue() * value_);
    }
    return super.mulImpl(other, reverse);
  }

  @Override
  protected Value<? extends Type, ?> divImpl(Value<? extends Type, ?> other, boolean reverse) {
    if (other.getType() == LongType.INSTANCE) {
      if (reverse) {
        return new LongValue(((LongValue) other).getValue() / value_);
      } else {
        return new LongValue(value_ / ((LongValue) other).getValue());
      }
    }
    return super.divImpl(other, reverse);
  }

  @Override
  protected Value<? extends Type, ?> modImpl(Value<? extends Type, ?> other, boolean reverse) {
    if (other.getType() == LongType.INSTANCE) {
      if (reverse) {
        return new LongValue(((LongValue) other).getValue() % value_);
      } else {
        return new LongValue(value_ % ((LongValue) other).getValue());
      }
    }
    return super.modImpl(other, reverse);
  }

  // compareToImpl
  @Override
  protected int compareToImpl(Value<? extends Type, ?> other, boolean reverse) {
    if (other.getType() == LongType.INSTANCE) {
      if (reverse) {
        return Long.compare(((LongValue) other).value_, value_);
      } else {
        return Long.compare(value_, ((LongValue) other).value_);
      }
    }

    if (other.getType() == DoubleType.INSTANCE) {
      if (reverse) {
        return Double.compare(((DoubleValue) other).value_, value_);
      } else {
        return Double.compare(value_, ((DoubleValue) other).value_);
      }
    }

    // float
    if (other.getType() == FloatType.INSTANCE) {
      if (reverse) {
        return Float.compare(((FloatValue) other).value_, value_);
      } else {
        return Float.compare(value_, ((FloatValue) other).value_);
      }
    }

    return super.compareToImpl(other, reverse);
  }
}
