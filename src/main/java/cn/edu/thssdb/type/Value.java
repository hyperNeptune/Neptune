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
