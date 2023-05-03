package cn.edu.thssdb.type;

import cn.edu.thssdb.utils.Global;

import java.nio.ByteBuffer;

// TODO: make bool available to users
public class BoolType extends Type {
  public static BoolType INSTANCE = new BoolType();

  static {
    Type.TYPES.put(INSTANCE.getTypeCode(), INSTANCE);
    Type.TYPEDICT.put("bool", INSTANCE);
  }

  @Override
  public byte getTypeCode() {
    return BuiltinTypeCode.BOOL.value;
  }

  @Override
  public int getTypeSize() {
    return Global.BOOL_SIZE;
  }

  @Override
  public int getTypeCodeSize() {
    return 1;
  }

  @Override
  public String toString() {
    return "bool";
  }

  @Override
  public Value<? extends Type, ?> getNullValue() {
    return BoolValue.NULL_VALUE;
  }

  @Override
  public Value<? extends Type, ?> castFrom(Value<? extends Type, ?> value) {
    if (value.isNull()) {
      return new BoolValue(false);
    }
    if (value.getType() == IntType.INSTANCE) {
      return new BoolValue(((IntValue) value).getValue() != 0);
    }
    if (value.getType() == LongType.INSTANCE) {
      return new BoolValue(((LongValue) value).getValue() != 0);
    }
    if (value.getType() == FloatType.INSTANCE) {
      return new BoolValue(((FloatValue) value).getValue() != 0);
    }
    if (value.getType() == DoubleType.INSTANCE) {
      return new BoolValue(((DoubleValue) value).getValue() != 0);
    }
    if (value.getType() == StringType.INSTANCE) {
      return new BoolValue(value.getValue() != "");
    }
    return super.castFrom(value);
  }

  @Override
  public Value<? extends Type, ?> deserializeValue(ByteBuffer buffer, int offset) {
    return new BoolValue(buffer.get(offset) == 1);
  }

  @Override
  protected Type customDeserialize(ByteBuffer buffer, int offset) {
    return this;
  }
}
