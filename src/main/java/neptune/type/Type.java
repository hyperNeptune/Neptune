package neptune.type;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

// THE CANONICAL NAME OF THESE TYPES ARE:
// INTEGER, BIGINT, DECIMAL, VARCHAR
// type can be an abstract factory
// TODO(wj): refactor null values as a new type
public abstract class Type {
  public static Map<Byte, Type> TYPES = new HashMap<>();
  public static Map<String, Type> TYPEDICT = new HashMap<>();

  // get type code
  public abstract byte getTypeCode();

  public abstract int getTypeSize();

  public abstract int getTypeCodeSize();

  // toString
  @Override
  public abstract String toString();

  // serialization
  public void serialize(ByteBuffer buffer, int offset) {
    buffer.put(offset, getTypeCode());
  }

  public void serialize(ByteBuffer buffer) {
    buffer.put(getTypeCode());
  }

  public abstract Value<? extends Type, ?> getNullValue();

  public Value<? extends Type, ?> castFrom(Value<? extends Type, ?> value) {
    // identical casting, ignore
    if (value.getType().getTypeCode() == getTypeCode()) {
      return value;
    }
    throw new RuntimeException("this casting is not allowed.");
  }

  // deserialization
  public abstract Value<? extends Type, ?> deserializeValue(ByteBuffer buffer, int offset);

  public static Type deserialize(ByteBuffer buffer, int offset) {
    Type temp = TYPES.get(buffer.get(offset));
    return temp.customDeserialize(buffer, offset + 1);
  }

  protected abstract Type customDeserialize(ByteBuffer buffer, int offset);
  // equals
  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (obj == this) {
      return true;
    }
    if (!(obj instanceof Type)) {
      return false;
    }
    Type rhs = (Type) obj;
    return getTypeCode() == rhs.getTypeCode();
  }
}
