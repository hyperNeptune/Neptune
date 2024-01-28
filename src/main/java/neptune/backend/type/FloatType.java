package neptune.backend.type;

import neptune.common.Global;

import java.nio.ByteBuffer;

public class FloatType extends Type {
  public static FloatType INSTANCE = new FloatType();

  static {
    Type.TYPEDICT.put("float", INSTANCE);
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
  public int getTypeCodeSize() {
    return 1;
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

  @Override
  public Value<? extends Type, ?> castFrom(Value<? extends Type, ?> value) {
    if (value.getType().getTypeCode() == BuiltinTypeCode.DOUBLE.value) {
      return new FloatValue(((DoubleValue) value).getValue().floatValue());
    }
    if (value.getType().getTypeCode() == BuiltinTypeCode.LONG.value) {
      return new FloatValue(((LongValue) value).getValue().floatValue());
    }
    if (value.getType().getTypeCode() == BuiltinTypeCode.INT.value) {
      return new FloatValue(((IntValue) value).getValue().floatValue());
    }
    return super.castFrom(value);
  }
}
