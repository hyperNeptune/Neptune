package neptune.backend.type;

import java.nio.ByteBuffer;
import java.util.function.Function;

public class StringValue extends Value<StringType.VarCharType, String> {
  private final String value_;
  public static final Function<Integer, StringValue> NULL_VALUE = StringValue::new;

  public StringValue(String value, int size) {
    super(new StringType.VarCharType(size));
    value_ = value;
  }

  public StringValue(int size) {
    super(new StringType.VarCharType(size));
    isNull_ = true;
    value_ = "";
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
    ((ByteBuffer) buffer.position(offset)).put(value_.getBytes()).clear();
  }

  @Override
  public String toString() {
    if (isNull_) {
      return "null";
    }
    return trimZeros(value_);
  }

  static String trimZeros(String str) {
    int pos = str.indexOf(0);
    return pos == -1 ? str : str.substring(0, pos);
  }

  @Override
  public String getValue() {
    return trimZeros(value_);
  }

  @Override
  protected Value<? extends Type, ?> addImpl(Value<? extends Type, ?> other, boolean reverse) {
    if (other.getType().getTypeCode() == BuiltinTypeCode.STRING.value) {
      return new StringValue(
          value_ + ((StringValue) other).getString(), other.getSize() + value_.length());
    }
    return super.addImpl(other, reverse);
  }
}
