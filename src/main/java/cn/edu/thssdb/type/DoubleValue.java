package cn.edu.thssdb.type;

import java.nio.ByteBuffer;

import cn.edu.thssdb.utils.Global;

public class DoubleValue extends Value{
  private double value_;

  // constructor
  public DoubleValue(double value) {
    super(Type.DOUBLE);
    value_ = value;
  }

  // getter
  public double getDouble() {
    return value_;
  }

  @Override
  public int compareTo(Value arg0) {
    if(arg0.getTypeId() == Type.DOUBLE) {
      return Double.compare(value_, ((DoubleValue)arg0).getDouble());
    }
    throw new UnsupportedOperationException("Unimplemented method 'compareTo'");
  }

  @Override
  public int getSize() {
    return Global.DOUBLE_SIZE;
  }

  @Override
  public void serialize(ByteBuffer buffer, int offset) {
    buffer.putDouble(offset, value_).rewind();
  }

  public static DoubleValue deserialize(ByteBuffer buffer, int offset) {
    return new DoubleValue(buffer.getDouble(offset));
  }

  @Override
  public Value add(Value lhs, Value rhs) {
    if(lhs.getTypeId() == Type.DOUBLE && rhs.getTypeId() == Type.DOUBLE) {
      double result = ((DoubleValue)lhs).getDouble() + ((DoubleValue)rhs).getDouble();
      return new DoubleValue(result);
    }
    throw new UnsupportedOperationException("Unimplemented method 'add'");
  }

  @Override
  public Value sub(Value lhs, Value rhs) {
    if(lhs.getTypeId() == Type.DOUBLE && rhs.getTypeId() == Type.DOUBLE) {
      double result = ((DoubleValue)lhs).getDouble() - ((DoubleValue)rhs).getDouble();
      return new DoubleValue(result);
    }
    throw new UnsupportedOperationException("Unimplemented method 'sub'");
  }

  @Override
  public Value mul(Value lhs, Value rhs) {
    if(lhs.getTypeId() == Type.DOUBLE && rhs.getTypeId() == Type.DOUBLE) {
      double result = ((DoubleValue)lhs).getDouble() * ((DoubleValue)rhs).getDouble();
      return new DoubleValue(result);
    }
    throw new UnsupportedOperationException("Unimplemented method 'mul'");
  }

  @Override
  public Value div(Value lhs, Value rhs) {
    if(lhs.getTypeId() == Type.DOUBLE && rhs.getTypeId() == Type.DOUBLE) {
      double result = ((DoubleValue)lhs).getDouble() / ((DoubleValue)rhs).getDouble();
      return new DoubleValue(result);
    }
    throw new UnsupportedOperationException("Unimplemented method 'div'");
  }

  @Override
  public Value mod(Value lhs, Value rhs) {
    if(lhs.getTypeId() == Type.DOUBLE && rhs.getTypeId() == Type.DOUBLE) {
      double result = ((DoubleValue)lhs).getDouble() % ((DoubleValue)rhs).getDouble();
      return new DoubleValue(result);
    }
    throw new UnsupportedOperationException("Unimplemented method 'mod'");
  }
  
}
