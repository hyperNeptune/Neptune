package cn.edu.thssdb.type;

import cn.edu.thssdb.utils.Global;

import java.nio.ByteBuffer;

public class DoubleType extends Type {
  public static DoubleType INSTANCE = new DoubleType();

  static {
    Type.TYPES.put(INSTANCE.getTypeCode(), INSTANCE);
  }

  @Override
  public byte getTypeCode() {
    return BuiltinTypeCode.DOUBLE.value;
  }

  @Override
  public int getTypeSize() {
    return Global.DOUBLE_SIZE;
  }

  @Override
  public String toString() {
    return "double";
  }

  @Override
  public Value<? extends DoubleType, Double> getNullValue() {
    return DoubleValue.NULL_VALUE;
  }

  @Override
  public Value<? extends DoubleType, Double> deserializeValue(ByteBuffer buffer, int offset) {
    return new DoubleValue(buffer.getDouble(offset));
  }

  @Override
  protected Type customDeserialize(ByteBuffer buffer, int offset) {
    return this;
  }
}