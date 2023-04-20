package cn.edu.thssdb.schema;

import cn.edu.thssdb.type.Type;
import cn.edu.thssdb.utils.Tuple2;

import java.nio.ByteBuffer;
import java.util.Map;

// Column class represents a column in a table
// | name | type | primary | nullable | maxLength | offset |
// separated by comma. so name can't contain comma
public class Column implements Comparable<Column> {
  private final String name_;
  private final Type type_;
  private final byte primary_;
  private final byte nullable_;
  private final int maxLength_;
  // offset in a tuple
  protected int offset_;

  public Column(
      String name, Type type, byte primary, byte nullable, int maxLength, int offset) {
    if (name.contains(",") || name.contains(";")) {
      // they will not in sql anyway
      throw new RuntimeException("Column name can't contain comma or semicolon!");
    }
    this.name_ = name;
    this.type_ = type;
    this.primary_ = primary;
    this.nullable_ = nullable;
    this.maxLength_ = maxLength;
    this.offset_ = offset;
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
  public int serialize(ByteBuffer buffer, int offset) {
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
    buffer.put(Byte.toString(primary_).getBytes(), offset, 1);
    offset += 1;
    buffer.put(offset, (byte) ',');
    offset += 1;
    // nullable
    buffer.put(Byte.toString(nullable_).getBytes(), offset, 1);
    offset += 1;
    buffer.put(offset, (byte) ',');
    offset += 1;
    // maxLength
    buffer.putInt(offset, maxLength_);
    offset += 4;
    // offset
    buffer.putInt(offset, offset_);
    offset += 4;
    return offset;
  }

  // calculate next ','
  public static int nextComma(ByteBuffer buffer, int offset) {
    int length = 0;
    while (buffer.get(offset + length) != ',') {
      length += 1;
    }
    return length;
  }

  // deserialize from ByteBuffer
  // WARNING: this method will change offset
  public static Tuple2<Column, Integer> deserialize(ByteBuffer buffer, Integer offset) {

    // name
    int length = nextComma(buffer, offset);
    String name = new String(buffer.array(), offset, length);
    offset += length + 1;
    // type
    length = nextComma(buffer, offset);
    Type type = Type.deserialize(buffer, offset);
    offset += length + 1;
    // primary
    byte primary = Byte.parseByte(new String(buffer.array(), offset, 1));
    offset += 1;
    // nullable
    byte nullable = Byte.parseByte(new String(buffer.array(), offset, 1));
    offset += 1;
    // maxLength
    int maxLength = buffer.getInt(offset);
    offset += 4;
    // offset
    int offset_ = buffer.getInt(offset);
    offset += 4;
    // if not ';' then error
    if (buffer.get(offset) != ';') {
      throw new RuntimeException("Column deserialize error!");
    }
    offset += 1;
    return new Tuple2<>(new Column(name, type, primary, nullable, maxLength, offset_), offset);
  }

  // getters
  public String getName() {
    return name_;
  }

  public Type getType() {
    return type_;
  }

  public byte isPrimary() {
    return primary_;
  }

  public byte Nullable() {
    return nullable_;
  }

  public int getMaxLength() {
    return maxLength_;
  }

  public int getOffset() {
    return offset_;
  }
}
