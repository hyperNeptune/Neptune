package cn.edu.thssdb.type;

import cn.edu.thssdb.utils.Global;

import java.nio.ByteBuffer;

public class IntType extends Type {
  public static IntType INSTANCE = new IntType();

  static {
    Type.TYPEDICT.put("int", INSTANCE);
    Type.TYPES.put(INSTANCE.getTypeCode(), INSTANCE);
  }

  @Override
  public byte getTypeCode() {
    return BuiltinTypeCode.INT.value;
  }

  @Override
  public String toString() {
    return "integer";
  }

  @Override
  public Value<? extends IntType, Integer> getNullValue() {
    return IntValue.NULL_VALUE;
  }

  @Override
  public Value<? extends IntType, Integer> deserializeValue(ByteBuffer buffer, int offset) {
    return new IntValue(buffer.getInt(offset));
  }

  @Override
  protected Type customDeserialize(ByteBuffer buffer, int offset) {
    return this;
  }

  @Override
  public int getTypeSize() {
    return Global.INT_SIZE;
  }

  @Override
  public int getTypeCodeSize() {
    return 1;
  }

  @Override
  public Value<? extends Type, ?> castFrom(Value<? extends Type, ?> value) {
    if (value.getType().getTypeCode() == BuiltinTypeCode.LONG.value) {
      return new IntValue(((LongValue) value).getValue().intValue());
    }
    return super.castFrom(value);
  }
}
