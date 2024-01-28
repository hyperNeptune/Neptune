package neptune.type;

import java.io.Serializable;
import java.nio.ByteBuffer;

// value is a interface for all values
// use abstract class to make it convenient
public abstract class Value<T extends Type, V>
    implements Serializable, Comparable<Value<? extends Type, ?>> {
  final Type type_;
  protected boolean isNull_ = false;

  // trivial constructor
  public Value(Type type) {
    type_ = type;
  }

  // clone is broken in java, so we use copy constructor
  public Value(Value<T, V> other) {
    type_ = other.type_;
    isNull_ = other.isNull_;
  }

  // get type id
  public Type getType() {
    return type_;
  }

  // get size
  public abstract int getSize();

  public boolean isNull() {
    return isNull_;
  }

  // write to byte buffer
  public abstract void serialize(ByteBuffer buffer, int offset);

  // toString
  @Override
  public abstract String toString();

  // arithmetic for self and other
  public final Value<? extends Type, ?> add(Value<? extends Type, ?> other) {
    try {
      return addImpl(other, false);
    } catch (UnsupportedOperationException e) {
      return other.addImpl(this, true);
    }
  }

  public final Value<? extends Type, ?> sub(Value<? extends Type, ?> other) {
    try {
      return subImpl(other, false);
    } catch (UnsupportedOperationException e) {
      return other.subImpl(this, true);
    }
  }

  public final Value<? extends Type, ?> mul(Value<? extends Type, ?> other) {
    try {
      return mulImpl(other, false);
    } catch (UnsupportedOperationException e) {
      return other.mulImpl(this, true);
    }
  }

  public final Value<? extends Type, ?> div(Value<? extends Type, ?> other) {
    try {
      return divImpl(other, false);
    } catch (UnsupportedOperationException e) {
      return other.divImpl(this, true);
    }
  }

  public final Value<? extends Type, ?> mod(Value<? extends Type, ?> other) {
    try {
      return modImpl(other, false);
    } catch (UnsupportedOperationException e) {
      return other.modImpl(this, true);
    }
  }

  // compare
  @Override
  public int compareTo(Value<? extends Type, ?> other) {
    try {
      return compareToImpl(other, false);
    } catch (UnsupportedOperationException e) {
      return other.compareToImpl(this, true);
    }
  }

  // arithmetic implementation
  // reverse means the order of lhs and rhs is reversed
  // we need this because old types can not 'add' new types
  // but new types can 'add' old types
  protected Value<? extends Type, ?> addImpl(Value<? extends Type, ?> other, boolean reverse) {
    throw new UnsupportedOperationException();
  }

  protected Value<? extends Type, ?> subImpl(Value<? extends Type, ?> other, boolean reverse) {
    throw new UnsupportedOperationException();
  }

  protected Value<? extends Type, ?> mulImpl(Value<? extends Type, ?> other, boolean reverse) {
    throw new UnsupportedOperationException();
  }

  protected Value<? extends Type, ?> divImpl(Value<? extends Type, ?> other, boolean reverse) {
    throw new UnsupportedOperationException();
  }

  protected Value<? extends Type, ?> modImpl(Value<? extends Type, ?> other, boolean reverse) {
    throw new UnsupportedOperationException();
  }

  protected int compareToImpl(Value<? extends Type, ?> other, boolean reverse) {
    throw new UnsupportedOperationException();
  }

  public abstract V getValue();

  // equals is not ==
  @Override
  public boolean equals(Object obj) {
    if (obj instanceof Value) {
      Value<?, ?> other = (Value<?, ?>) obj;
      if (isNull_ && other.isNull_) {
        return true;
      }
      if (isNull_ || other.isNull_) {
        return false;
      }
      return compareTo(other) == 0;
    }
    return false;
  }
}
