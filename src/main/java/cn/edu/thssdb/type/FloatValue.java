package cn.edu.thssdb.type;

import cn.edu.thssdb.utils.Global;

import java.nio.ByteBuffer;

public class FloatValue extends Value<FloatType, Float> {
  float value_;
  public static final FloatValue NULL_VALUE = new FloatValue();

  // constructor
  public FloatValue(float value) {
    super(FloatType.INSTANCE);
    value_ = value;
  }

  public FloatValue() {
    super(FloatType.INSTANCE);
    isNull_ = true;
    value_ = 0;
  }

  @Override
  public int getSize() {
    return Global.FLOAT_SIZE;
  }

  @Override
  public void serialize(ByteBuffer buffer, int offset) {
    buffer.putFloat(offset, value_);
  }

  @Override
  public String toString() {
    if (isNull_) {
      return "null (float)";
    }
    return Float.toString(value_);
  }

  @Override
  protected Value<? extends Type, ?> addImpl(Value<? extends Type, ?> other, boolean reverse) {
    if (other.getType() == FloatType.INSTANCE) {
      return new FloatValue(((FloatValue) other).getValue() + value_);
    }
    return super.addImpl(other, reverse);
  }

  @Override
  protected Value<? extends Type, ?> subImpl(Value<? extends Type, ?> other, boolean reverse) {
    if (other.getType() == FloatType.INSTANCE) {
      if (reverse) {
        return new FloatValue(((FloatValue) other).getValue() - value_);
      } else {
        return new FloatValue(value_ - ((FloatValue) other).getValue());
      }
    }
    return super.subImpl(other, reverse);
  }

  @Override
  protected Value<? extends Type, ?> mulImpl(Value<? extends Type, ?> other, boolean reverse) {
    if (other.getType() == FloatType.INSTANCE) {
      return new FloatValue(((FloatValue) other).getValue() * value_);
    }
    return super.mulImpl(other, reverse);
  }

  @Override
  protected Value<? extends Type, ?> divImpl(Value<? extends Type, ?> other, boolean reverse) {
    if (other.getType() == FloatType.INSTANCE) {
      if (reverse) {
        return new FloatValue(((FloatValue) other).getValue() / value_);
      } else {
        return new FloatValue(value_ / ((FloatValue) other).getValue());
      }
    }
    return super.divImpl(other, reverse);
  }

  @Override
  protected Value<? extends Type, ?> modImpl(Value<? extends Type, ?> other, boolean reverse) {
    if (other.getType() == FloatType.INSTANCE) {
      if (reverse) {
        return new FloatValue(((FloatValue) other).getValue() % value_);
      } else {
        return new FloatValue(value_ % ((FloatValue) other).getValue());
      }
    }
    return super.modImpl(other, reverse);
  }

  @Override
  public Float getValue() {
    return value_;
  }

  @Override
  protected int compareToImpl(Value<? extends Type, ?> other, boolean reverse) {
    if (other.getType() == FloatType.INSTANCE) {
      if (reverse) {
        return Float.compare(((FloatValue) other).getValue(), value_);
      } else {
        return Float.compare(value_, ((FloatValue) other).getValue());
      }
    }
    return super.compareToImpl(other, reverse);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof DoubleValue) {
      if (((DoubleValue) obj).getValue() > Global.FLOAT_MAX) {
        return false;
      } else if (((DoubleValue) obj).getValue() < Global.FLOAT_MIN) {
        return false;
      } else {
        return value_ == ((DoubleValue) obj).getValue();
      }
    }
    return super.equals(obj);
  }
}
