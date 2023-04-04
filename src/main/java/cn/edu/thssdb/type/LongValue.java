package cn.edu.thssdb.type;

import cn.edu.thssdb.utils.Global;

import java.nio.ByteBuffer;

public class LongValue extends Value {
  private final long value_;

  public LongValue(long value) {
    super(Type.LONG);
    value_ = value;
  }

  public long getLong() {
    return value_;
  }

  @Override
  public int compareTo(Value arg0) {
    if (arg0.getTypeId() == Type.LONG) {
      return Long.compare(value_, ((LongValue) arg0).getLong());
    }
    throw new UnsupportedOperationException("Unimplemented method 'compareTo'");
  }

  @Override
  public int getSize() {
    return Global.LONG_SIZE;
  }

  @Override
  public void serialize(ByteBuffer buffer, int offset) {
    buffer.putLong(offset, value_);
  }

  public static LongValue deserialize(ByteBuffer buffer, int offset) {
    return new LongValue(buffer.getLong(offset));
  }

  @Override
  public Value add(Value lhs, Value rhs) {
    if (lhs.getTypeId() == Type.LONG && rhs.getTypeId() == Type.LONG) {
      long result = ((LongValue) lhs).getLong() + ((LongValue) rhs).getLong();
      return new LongValue(result);
    }
    throw new UnsupportedOperationException("Unimplemented method 'add'");
  }

  @Override
  public Value sub(Value lhs, Value rhs) {
    if (lhs.getTypeId() == Type.LONG && rhs.getTypeId() == Type.LONG) {
      long result = ((LongValue) lhs).getLong() - ((LongValue) rhs).getLong();
      return new LongValue(result);
    }
    throw new UnsupportedOperationException("Unimplemented method 'sub'");
  }

  @Override
  public Value mul(Value lhs, Value rhs) {
    if (lhs.getTypeId() == Type.LONG && rhs.getTypeId() == Type.LONG) {
      long result = ((LongValue) lhs).getLong() * ((LongValue) rhs).getLong();
      return new LongValue(result);
    }
    throw new UnsupportedOperationException("Unimplemented method 'mul'");
  }

  @Override
  public Value div(Value lhs, Value rhs) {
    if (lhs.getTypeId() == Type.LONG && rhs.getTypeId() == Type.LONG) {
      long result = ((LongValue) lhs).getLong() / ((LongValue) rhs).getLong();
      return new LongValue(result);
    }
    throw new UnsupportedOperationException("Unimplemented method 'div'");
  }

  @Override
  public Value mod(Value lhs, Value rhs) {
    if (lhs.getTypeId() == Type.LONG && rhs.getTypeId() == Type.LONG) {
      long result = ((LongValue) lhs).getLong() % ((LongValue) rhs).getLong();
      return new LongValue(result);
    }
    throw new UnsupportedOperationException("Unimplemented method 'mod'");
  }
}
