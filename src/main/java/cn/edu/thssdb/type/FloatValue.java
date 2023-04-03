package cn.edu.thssdb.type;

import java.nio.ByteBuffer;

import cn.edu.thssdb.utils.Global;

public class FloatValue extends Value{
  private float value_;

  public FloatValue(float value) {
    super(Type.FLOAT);
    value_ = value;
  }

  public float getFloat() {
    return value_;
  }

  @Override
  public int compareTo(Value arg0) {
    if (arg0.getTypeId() == Type.FLOAT) {
      return Float.compare(value_, ((FloatValue)arg0).getFloat());
    }
    throw new UnsupportedOperationException("Unimplemented method 'compareTo'");
  }

  @Override
  public int getSize() {
    return Global.FLOAT_SIZE;
  }

  @Override
  public void serialize(ByteBuffer buffer, int offset) {
    buffer.putFloat(offset, value_).rewind();
  }

  @Override
  public Value add(Value lhs, Value rhs) {
    if (lhs.getTypeId() == Type.FLOAT && rhs.getTypeId() == Type.FLOAT) {
      float result = ((FloatValue)lhs).getFloat() + ((FloatValue)rhs).getFloat();
      return new FloatValue(result);
    }
    throw new UnsupportedOperationException("Unimplemented method 'add'");
  }

  @Override
  public Value sub(Value lhs, Value rhs) {
    if (lhs.getTypeId() == Type.FLOAT && rhs.getTypeId() == Type.FLOAT) {
      float result = ((FloatValue)lhs).getFloat() - ((FloatValue)rhs).getFloat();
      return new FloatValue(result);
    }
    throw new UnsupportedOperationException("Unimplemented method 'sub'");
  }

  @Override
  public Value mul(Value lhs, Value rhs) {
    if (lhs.getTypeId() == Type.FLOAT && rhs.getTypeId() == Type.FLOAT) {
      float result = ((FloatValue)lhs).getFloat() * ((FloatValue)rhs).getFloat();
      return new FloatValue(result);
    }
    throw new UnsupportedOperationException("Unimplemented method 'mul'");
  }

  @Override
  public Value div(Value lhs, Value rhs) {
    if (lhs.getTypeId() == Type.FLOAT && rhs.getTypeId() == Type.FLOAT) {
      float result = ((FloatValue)lhs).getFloat() / ((FloatValue)rhs).getFloat();
      return new FloatValue(result);
    }
    throw new UnsupportedOperationException("Unimplemented method 'div'");
  }

  @Override
  public Value mod(Value lhs, Value rhs) {
    if (lhs.getTypeId() == Type.FLOAT && rhs.getTypeId() == Type.FLOAT) {
      float result = ((FloatValue)lhs).getFloat() % ((FloatValue)rhs).getFloat();
      return new FloatValue(result);
    }
    throw new UnsupportedOperationException("Unimplemented method 'mod'");
  }
  
}
