package cn.edu.thssdb.type;

import cn.edu.thssdb.utils.Global;

import java.nio.ByteBuffer;

public class DoubleValue extends Value<DoubleType, Double> {
  double value_;
  public static final DoubleValue NULL_VALUE = new DoubleValue();

  // constructor
  public DoubleValue(double value) {
    super(DoubleType.INSTANCE);
    value_ = value;
  }

  public DoubleValue() {
    super(DoubleType.INSTANCE);
    isNull_ = true;
    value_ = 0;
  }

  @Override
  public int getSize() {
    return Global.DOUBLE_SIZE;
  }

  @Override
  public void serialize(ByteBuffer buffer, int offset) {
    buffer.putDouble(offset, value_);
  }

  @Override
  public String toString() {
    if (isNull_) {
      return "null (double)";
    }
    return Double.toString(value_);
  }

  @Override
  protected Value<? extends Type, ?> addImpl(Value<? extends Type, ?> other, boolean reverse) {
    if (other.getType() == DoubleType.INSTANCE) {
      return new DoubleValue(((DoubleValue) other).getValue() + value_);
    }
    return super.addImpl(other, reverse);
  }

  @Override
  protected Value<? extends Type, ?> subImpl(Value<? extends Type, ?> other, boolean reverse) {
    if (other.getType() == DoubleType.INSTANCE) {
      if (reverse) {
        return new DoubleValue(((DoubleValue) other).getValue() - value_);
      } else {
        return new DoubleValue(value_ - ((DoubleValue) other).getValue());
      }
    }
    return super.subImpl(other, reverse);
  }

  @Override
  protected Value<? extends Type, ?> mulImpl(Value<? extends Type, ?> other, boolean reverse) {
    if (other.getType() == DoubleType.INSTANCE) {
      return new DoubleValue(((DoubleValue) other).getValue() * value_);
    }
    return super.mulImpl(other, reverse);
  }

  @Override
  protected Value<? extends Type, ?> divImpl(Value<? extends Type, ?> other, boolean reverse) {
    if (other.getType() == DoubleType.INSTANCE) {
      if (reverse) {
        return new DoubleValue(((DoubleValue) other).getValue() / value_);
      } else {
        return new DoubleValue(value_ / ((DoubleValue) other).getValue());
      }
    }
    return super.divImpl(other, reverse);
  }

  @Override
  protected Value<? extends Type, ?> modImpl(Value<? extends Type, ?> other, boolean reverse) {
    if (other.getType() == DoubleType.INSTANCE) {
      if (reverse) {
        return new DoubleValue(((DoubleValue) other).getValue() % value_);
      } else {
        return new DoubleValue(value_ % ((DoubleValue) other).getValue());
      }
    }

    if (other.getType() == FloatType.INSTANCE) {
      if (reverse) {
        return new DoubleValue(((FloatValue) other).getValue() % value_);
      } else {
        return new DoubleValue(value_ % ((FloatValue) other).getValue());
      }
    }

    return super.modImpl(other, reverse);
  }

  // getValue, compareToImpl
  @Override
  public Double getValue() {
    return value_;
  }

  @Override
  protected int compareToImpl(Value<? extends Type, ?> other, boolean reverse) {
    if (other.getType() == DoubleType.INSTANCE) {
      if (reverse) {
        return Double.compare(((DoubleValue) other).getValue(), value_);
      } else {
        return Double.compare(value_, ((DoubleValue) other).getValue());
      }
    }
    return super.compareToImpl(other, reverse);
  }
}
