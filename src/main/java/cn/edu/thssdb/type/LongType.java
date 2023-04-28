package cn.edu.thssdb.type;

import cn.edu.thssdb.utils.Global;

import java.nio.ByteBuffer;

public class LongType extends Type {
  public static LongType INSTANCE = new LongType();

  static {
    Type.TYPES.put(INSTANCE.getTypeCode(), INSTANCE);
  }

  @Override
  public byte getTypeCode() {
    return BuiltinTypeCode.LONG.value;
  }

  @Override
  public int getTypeSize() {
    return Global.LONG_SIZE;
  }

  @Override
  public int getTypeCodeSize() {
    return 1;
  }

  @Override
  public String toString() {
    return "long";
  }

  @Override
  public Value<? extends LongType, Long> getNullValue() {
    return LongValue.NULL_VALUE;
  }

  @Override
  public Value<? extends LongType, Long> deserializeValue(ByteBuffer buffer, int offset) {
    return new LongValue(buffer.getLong(offset));
  }

  @Override
  protected Type customDeserialize(ByteBuffer buffer, int offset) {
    return this;
  }
}
