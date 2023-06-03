package cn.edu.thssdb.schema;

import cn.edu.thssdb.type.Type;
import cn.edu.thssdb.utils.Pair;

import java.nio.ByteBuffer;

// Column class represents a column in a table
// | name | type | primary | nullable | maxLength | offset |
// separated by comma. so name can't contain comma
public class Column implements Comparable<Column> {
  private final String name_;
  private String fullname_;
  private final Type type_;
  private byte primary_;
  private final byte nullable_;
  private final int maxLength_;
  // offset in a tuple
  protected int offset_;

  public Column(String name, Type type, byte primary, byte nullable, int maxLength, int offset) {
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

  // copy constructor
  public Column(Column c) {
    this.name_ = c.name_;
    this.type_ = c.type_;
    this.primary_ = c.primary_;
    this.nullable_ = c.nullable_;
    this.maxLength_ = c.maxLength_;
    this.offset_ = c.offset_;
    if (fullname_ != null) {
      this.fullname_ = c.fullname_;
    }
  }

  // rename factory
  public static Column ColumnRenameFactory(Column c, String newName) {
    Column col = new Column(
        newName,
        c.type_,
        c.primary_,
        c.nullable_,
        c.maxLength_,
        c.offset_);
    col.setFullname(c.getFullname());
    return col;
  }

  public void setFullNameByTable(String tableName) {
    this.fullname_ = tableName + "." + name_;
  }
  public void setFullname(String fullname) {
    this.fullname_ = fullname;
  }

  // must check whether retv == null
  public String getFullname() {
    return fullname_;
  }

  @Override
  public int compareTo(Column e) {
    return name_.compareTo(e.name_);
  }

  public void setPrimary(byte primary) {
    this.primary_ = primary;
  }

  public byte getPrimary() {
    return primary_;
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
    buffer.position(offset);
    // name
    buffer.put(name_.getBytes());
    buffer.put((byte) ',');

    // type
    type_.serialize(buffer);
    buffer.put((byte) ',');

    // primary
    buffer.put(primary_);

    // nullable
    buffer.put(nullable_);

    // maxLength
    buffer.putInt(maxLength_);

    // offset
    buffer.putInt(offset_);

    int retv = buffer.position();
    buffer.clear();
    return retv;
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
  public static Pair<Column, Integer> deserialize(ByteBuffer buffer, Integer offset) {
    // name
    int length = nextComma(buffer, offset);
    String name = new String(buffer.array(), offset, length);
    offset += length + 1;

    // type
    length = nextComma(buffer, offset);
    Type type = Type.deserialize(buffer, offset);
    offset += length + 1;

    // primary
    byte primary = buffer.get(offset);
    offset += 1;

    // nullable
    byte nullable = buffer.get(offset);
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
    return new Pair<>(new Column(name, type, primary, nullable, maxLength, offset_), offset);
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

  public int getColMetaSize() {
    return name_.getBytes().length
        + 1 // comma
        + type_.getTypeCodeSize()
        + 1 // comma
        + 1
        + 1
        + 4
        + 4 // meta info
        + 1; // ;
  }

  public int getOffset() {
    return offset_;
  }
}
