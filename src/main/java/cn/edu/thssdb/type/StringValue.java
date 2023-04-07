package cn.edu.thssdb.type;

import java.nio.ByteBuffer;
import java.util.function.Function;

public class StringValue extends Value<StringType.VarCharType, String> {
  private final String value_;
  public static final Function<Integer, StringValue> NULL_VALUE =
      (size) -> new StringValue("", size);

  public StringValue(String value, int size) {
    super(new StringType.VarCharType(size));
    value_ = value;
  }

  public String getString() {
    return value_;
  }

  @Override
  public int compareToImpl(Value<? extends Type, ?> arg0, boolean reverse) {
    if (arg0.getType().equals(this.getType())) {
      return value_.compareTo(((StringValue) arg0).getString());
    }
    throw new UnsupportedOperationException("Unimplemented method 'compareTo'");
  }

  @Override
  public int getSize() {
    return value_.length();
  }

  @Override
  public void serialize(ByteBuffer buffer, int offset) {
    ((ByteBuffer) buffer.position(offset)).put(value_.getBytes());
  }

  @Override
  public String toString() {
    return value_;
  }

  @Override
  public String getValue() {
    return value_;
  }

  @Override
  protected Value<? extends Type, ?> addImpl(Value<? extends Type, ?> other, boolean reverse) {
    if (other.getType().getTypeCode() == BuiltinTypeCode.STRING.value) {
      return new StringValue(
          ((StringValue) other).getString() + value_, other.getSize() + value_.length());
    }
    return super.addImpl(other, reverse);
  }
}
