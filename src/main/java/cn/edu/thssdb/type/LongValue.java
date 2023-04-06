package cn.edu.thssdb.type;

import cn.edu.thssdb.utils.Global;

import java.nio.ByteBuffer;

public class LongValue extends Value<LongType, Long> {

  long value_;

  public static final LongValue NULL_VALUE = new LongValue(0L);

  // constructor
  public LongValue(long value) {
    super(LongType.INSTANCE);
    value_ = value;
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
        return Long.compare(((IntValue) other).value_, value_);
      } else {
        return Long.compare(value_, ((IntValue) other).value_);
      }
    }
    return super.compareToImpl(other, reverse);
  }
}
