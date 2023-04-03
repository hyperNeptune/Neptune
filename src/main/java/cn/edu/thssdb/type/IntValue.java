package cn.edu.thssdb.type;

import java.nio.ByteBuffer;

import cn.edu.thssdb.utils.Global;

public class IntValue extends Value{
  private int value_;

  public IntValue(int value) {
    super(Type.INT);
    value_ = value;
  }

  public int getInt() {
    return value_;
  }

  // arithmetic
  @Override
  public Value add(Value lhs, Value rhs) {
    if(lhs.getTypeId() == Type.INT && rhs.getTypeId() == Type.INT) {
      int result = ((IntValue)lhs).getInt() + ((IntValue)rhs).getInt();
      return new IntValue(result);
    }
    throw new UnsupportedOperationException("Unimplemented method 'add'");
  }

  @Override
  public Value sub(Value lhs, Value rhs) {
    if(lhs.getTypeId() == Type.INT && rhs.getTypeId() == Type.INT) {
      int result = ((IntValue)lhs).getInt() - ((IntValue)rhs).getInt();
      return new IntValue(result);
    }
    throw new UnsupportedOperationException("Unimplemented method 'sub'");
  }

  @Override
  public Value mul(Value lhs, Value rhs) {
    if(lhs.getTypeId() == Type.INT && rhs.getTypeId() == Type.INT) {
      int result = ((IntValue)lhs).getInt() * ((IntValue)rhs).getInt();
      return new IntValue(result);
    }
    throw new UnsupportedOperationException("Unimplemented method 'mul'");
  }

  @Override
  public Value div(Value lhs, Value rhs) {
    if(lhs.getTypeId() == Type.INT && rhs.getTypeId() == Type.INT) {
      int result = ((IntValue)lhs).getInt() / ((IntValue)rhs).getInt();
      return new IntValue(result);
    }
    throw new UnsupportedOperationException("Unimplemented method 'div'");
  }

  @Override
  public Value mod(Value lhs, Value rhs) {
    if(lhs.getTypeId() == Type.INT && rhs.getTypeId() == Type.INT) {
      int result = ((IntValue)lhs).getInt() % ((IntValue)rhs).getInt();
      return new IntValue(result);
    }
    throw new UnsupportedOperationException("Unimplemented method 'mod'");
  }

  @Override
  public int compareTo(Value arg0) {
    if(arg0.getTypeId() == Type.INT) {
      return Integer.compare(this.value_, ((IntValue)arg0).getInt());
    }
    throw new UnsupportedOperationException("Unimplemented method 'compareTo'");
  }

  @Override
  public void serialize(ByteBuffer buffer, int offset) {
    buffer.putInt(offset, value_).rewind();
  }

  public static IntValue deserialize(ByteBuffer buffer, int offset) {
    return new IntValue(buffer.getInt(offset));
  }

	@Override
	public int getSize() {
		return Global.INT_SIZE;
	}

}
