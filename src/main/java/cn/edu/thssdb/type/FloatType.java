package cn.edu.thssdb.type;

import cn.edu.thssdb.utils.Global;

import java.nio.ByteBuffer;

public class FloatType extends Type {
  public static FloatType INSTANCE = new FloatType();

  static {
    Type.TYPES.put(INSTANCE.getTypeCode(), INSTANCE);
  }

  @Override
  public byte getTypeCode() {
    return BuiltinTypeCode.FLOAT.value;
  }

  @Override
  public int getTypeSize() {
    return Global.FLOAT_SIZE;
  }

  @Override
  public String toString() {
    return "float";
  }

  @Override
  public Value<? extends FloatType, Float> getNullValue() {
    return FloatValue.NULL_VALUE;
  }

  @Override
  public Value<? extends FloatType, Float> deserializeValue(ByteBuffer buffer, int offset) {
    return new FloatValue(buffer.getFloat(offset));
  }

  @Override
  protected Type customDeserialize(ByteBuffer buffer, int offset) {
    return this;
  }
}
