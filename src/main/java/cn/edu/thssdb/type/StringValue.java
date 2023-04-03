package cn.edu.thssdb.type;

import java.nio.ByteBuffer;

public class StringValue extends Value {
  private String value_;

  public StringValue(String value) {
    super(Type.STRING);
    value_ = value;
  }

  public String getString() {
    return value_;
  }

  @Override
  public int compareTo(Value arg0) {
    if (arg0.getTypeId() == Type.STRING) {
      return value_.compareTo(((StringValue) arg0).getString());
    }
    throw new UnsupportedOperationException("Unimplemented method 'compareTo'");
  }

  @Override
  public int getSize() {
    return value_.length();
  }

  @Override
  public void serialize(ByteBuffer buffer, int offset) {
    ((ByteBuffer) buffer.position(offset)).put(value_.getBytes()).rewind();
  }

  public static StringValue deserialize(ByteBuffer buffer, int offset) {
    return new StringValue(new String(buffer.array(), offset, buffer.array().length - offset));
  }

  @Override
  public Value add(Value lhs, Value rhs) {
    if (lhs.getTypeId() == Type.STRING && rhs.getTypeId() == Type.STRING) {
      String result = ((StringValue) lhs).getString() + ((StringValue) rhs).getString();
      return new StringValue(result);
    }
    throw new UnsupportedOperationException("Unimplemented method 'add'");
  }

  @Override
  public Value sub(Value lhs, Value rhs) {
    throw new UnsupportedOperationException("Unimplemented method 'sub'");
  }

  @Override
  public Value mul(Value lhs, Value rhs) {
    throw new UnsupportedOperationException("Unimplemented method 'mul'");
  }

  @Override
  public Value div(Value lhs, Value rhs) {
    throw new UnsupportedOperationException("Unimplemented method 'div'");
  }

  @Override
  public Value mod(Value lhs, Value rhs) {
    throw new UnsupportedOperationException("Unimplemented method 'mod'");
  }
}
