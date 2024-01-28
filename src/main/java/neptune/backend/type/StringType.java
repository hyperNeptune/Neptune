package neptune.backend.type;

import neptune.common.Global;

import java.nio.ByteBuffer;

// StringType produces VarCharType, and VarCharType produces StringValue.
public class StringType extends Type {

  public static StringType INSTANCE = new StringType();

  static {
    Type.TYPEDICT.put("string", INSTANCE);
    Type.TYPES.put(INSTANCE.getTypeCode(), INSTANCE);
  }

  @Override
  public byte getTypeCode() {
    return BuiltinTypeCode.STRING.value;
  }

  @Override
  public int getTypeSize() {
    throw new RuntimeException("StringType.getTypeSize() should not be called");
  }

  @Override
  public int getTypeCodeSize() {
    return Global.INT_SIZE + 1;
  }

  @Override
  public String toString() {
    return "string";
  }

  @Override
  public Value<? extends StringType, String> getNullValue() {
    throw new RuntimeException("StringType.getNullValue() should not be called");
  }

  @Override
  public Value<? extends StringType, String> deserializeValue(ByteBuffer buffer, int offset) {
    throw new RuntimeException("StringType.deserializeValue() should not be called");
  }

  @Override
  protected Type customDeserialize(ByteBuffer buffer, int offset) {
    return new VarCharType(buffer.getInt(offset));
  }

  public static Type getVarCharType(int i) {
    return new VarCharType(i);
  }

  static class VarCharType extends StringType {
    private final int size_;

    public VarCharType(int size) {
      size_ = size;
    }

    @Override
    public int getTypeSize() {
      return size_;
    }

    @Override
    public String toString() {
      return "varchar(" + size_ + ")";
    }

    @Override
    public Value<? extends VarCharType, String> getNullValue() {
      return StringValue.NULL_VALUE.apply(size_);
    }

    @Override
    public void serialize(ByteBuffer buffer) {
      super.serialize(buffer);
      buffer.putInt(this.size_);
    }

    @Override
    public void serialize(ByteBuffer buffer, int offset) {
      super.serialize(buffer, offset);
      buffer.putInt(this.size_, offset + 1);
    }

    @Override
    public Value<? extends VarCharType, String> deserializeValue(ByteBuffer buffer, int offset) {
      String s;
      if (size_ == 0) {
        s = "";
      } else {
        s = new String(buffer.array(), offset, size_);
      }
      return new StringValue(s, size_);
    }

    @Override
    public boolean equals(Object obj) {
      if (obj instanceof VarCharType) {
        return size_ == ((VarCharType) obj).size_;
      }
      return super.equals(obj);
    }
  }
}
