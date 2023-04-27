package cn.edu.thssdb.type;

import cn.edu.thssdb.utils.Global;

import java.nio.ByteBuffer;

public class IntValue extends Value<IntType, Integer> {
  int value_;
  public static final IntValue NULL_VALUE = new IntValue();

  // constructor
  public IntValue(int value) {
    super(IntType.INSTANCE);
    value_ = value;
  }

  public IntValue() {
    super(IntType.INSTANCE);
    isNull_ = true;
    value_ = 0;
  }

  @Override
  public int getSize() {
    return Global.INT_SIZE;
  }

  @Override
  public void serialize(ByteBuffer buffer, int offset) {
    buffer.putInt(offset, value_);
  }

  @Override
  public String toString() {
    if (isNull_) {
      return "null (int)";
    }
    return Integer.toString(value_);
  }

  @Override
  protected Value<? extends Type, ?> addImpl(Value<? extends Type, ?> other, boolean reverse) {
    if (other.getType() == IntType.INSTANCE) {
      return new IntValue(((IntValue) other).getValue() + value_);
    }
    return super.addImpl(other, reverse);
  }

  @Override
  protected Value<? extends Type, ?> subImpl(Value<? extends Type, ?> other, boolean reverse) {
    if (other.getType() == IntType.INSTANCE) {
      if (reverse) {
        return new IntValue(((IntValue) other).getValue() - value_);
      } else {
        return new IntValue(value_ - ((IntValue) other).getValue());
      }
    }
    return super.subImpl(other, reverse);
  }

  @Override
  protected Value<? extends Type, ?> mulImpl(Value<? extends Type, ?> other, boolean reverse) {
    if (other.getType() == IntType.INSTANCE) {
      return new IntValue(((IntValue) other).getValue() * value_);
    }
    return super.mulImpl(other, reverse);
  }

  @Override
  protected Value<? extends Type, ?> divImpl(Value<? extends Type, ?> other, boolean reverse) {
    if (other.getType() == IntType.INSTANCE) {
      if (reverse) {
        return new IntValue(((IntValue) other).getValue() / value_);
      } else {
        return new IntValue(value_ / ((IntValue) other).getValue());
      }
    }
    return super.divImpl(other, reverse);
  }

  @Override
  protected Value<? extends Type, ?> modImpl(Value<? extends Type, ?> other, boolean reverse) {
    if (other.getType() == IntType.INSTANCE) {
      if (reverse) {
        return new IntValue(((IntValue) other).getValue() % value_);
      } else {
        return new IntValue(value_ % ((IntValue) other).getValue());
      }
    }
    return super.modImpl(other, reverse);
  }

  @Override
  public Integer getValue() {
    return value_;
  }

  @Override
  protected int compareToImpl(Value<? extends Type, ?> other, boolean reverse) {
    if (other.getType() == IntType.INSTANCE) {
      if (reverse) {
        return Integer.compare(((IntValue) other).value_, value_);
      } else {
        return Integer.compare(value_, ((IntValue) other).value_);
      }
    }
    return super.compareToImpl(other, reverse);
  }
}
