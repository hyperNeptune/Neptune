package cn.edu.thssdb.schema;

import cn.edu.thssdb.type.Type;

import java.nio.ByteBuffer;

// Column class represents a column in a table
// | name | type | primary | nullable | maxLength | offset |
// separated by comma. so name can't contain comma
public class Column implements Comparable<Column> {
  private String name_;
  private Type type_;
  private Boolean primary_;
  private Boolean nullable_;
  private int maxLength_;
  // offset in a tuple
  protected int offset_;

  public Column(String name, Type type, Boolean primary, boolean nullable, int maxLength) {
    if (name.contains(",") || name.contains(";")) {
      throw new RuntimeException("Column name can't contain comma or semicolon!");
    }
    this.name_ = name;
    this.type_ = type;
    this.primary_ = primary;
    this.nullable_ = nullable;
    this.maxLength_ = maxLength;
  }

  @Override
  public int compareTo(Column e) {
    return name_.compareTo(e.name_);
  }

  // human-readable string with variable names
  public String toString() {
    return "Column{"
        + "name='"
        + name_
        + '\''
        + ", type="
        + type_
        + ", primary="
        + primary_
        + ", nullable="
        + nullable_
        + ", maxLength="
        + maxLength_
        + ", offset="
        + offset_
        + '}';
  }

  // serialize to ByteBuffer, comma separated
  public void serialize(ByteBuffer buffer, int offset) {
    // name
    buffer.put(name_.getBytes(), offset, name_.length());
    offset += name_.length();
    buffer.put(offset, (byte) ',');
    offset += 1;
    // type
    type_.serialize(buffer, offset);
    offset += 1;
    buffer.put(offset, (byte) ',');
    offset += 1;
    // primary
    buffer.put(primary_.toString().getBytes(), offset, primary_.toString().length());
    offset += primary_.toString().length();
    buffer.put(offset, (byte) ',');
    offset += 1;
    // nullable
    buffer.put(nullable_.toString().getBytes(), offset, nullable_.toString().length());
    offset += nullable_.toString().length();
    buffer.put(offset, (byte) ',');
    offset += 1;
    // maxLength
    buffer.putInt(offset, maxLength_);
    offset += 4;
    // offset
    buffer.putInt(offset, offset_);
  }

  // deserialize from ByteBuffer
  public static Column deserialize(ByteBuffer buffer, int offset) {
    // TODO
    return null;
  }

  // getters
  public String getName() {
    return name_;
  }

  public Type getType() {
    return type_;
  }

  public Boolean isPrimary() {
    return primary_;
  }

  public Boolean Nullable() {
    return nullable_;
  }

  public int getMaxLength() {
    return maxLength_;
  }

  public int getOffset() {
    return offset_;
  }
}
