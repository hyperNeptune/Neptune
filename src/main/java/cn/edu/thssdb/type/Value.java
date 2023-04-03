package cn.edu.thssdb.type;

import java.io.Serializable;
import java.nio.ByteBuffer;

// value is a interface for all values
public abstract class Value implements Serializable, Cloneable, Comparable<Value> {
  Type typeId_;

  // trival constructor
  public Value(Type typeId) {
    typeId_ = typeId;
  }

  // get type id
  public Type getTypeId() {
    return typeId_;
  }

  // get size
  public abstract int getSize();

  // write to byte buffer
  public abstract void serialize(ByteBuffer buffer, int offset);

  public static Value deserialize(ByteBuffer buffer, int offset, Type typeId) {
    switch (typeId) {
      case INT:
        return IntValue.deserialize(buffer, offset);
      case LONG:
        return LongValue.deserialize(buffer, offset);
      case FLOAT:
        return FloatValue.deserialize(buffer, offset);
      case DOUBLE:
        return DoubleValue.deserialize(buffer, offset);
      case STRING:
        return StringValue.deserialize(buffer, offset);
      default:
        throw new UnsupportedOperationException("Unimplemented method 'deserialize'");
    }
  }
  // arithmetic operations
  public abstract Value add(Value lhs, Value rhs);

  public abstract Value sub(Value lhs, Value rhs);

  public abstract Value mul(Value lhs, Value rhs);

  public abstract Value div(Value lhs, Value rhs);

  public abstract Value mod(Value lhs, Value rhs);

  // arithmetic for self and other
  public Value add(Value other) {
    return add(this, other);
  }

  public Value sub(Value other) {
    return sub(this, other);
  }

  public Value mul(Value other) {
    return mul(this, other);
  }

  public Value div(Value other) {
    return div(this, other);
  }

  public Value mod(Value other) {
    return mod(this, other);
  }
}
