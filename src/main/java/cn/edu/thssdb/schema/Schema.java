package cn.edu.thssdb.schema;

import java.io.Serializable;
import java.nio.ByteBuffer;

// assume all columns are inlined
// schema is a list of columns
// |---------|---------|---------|-----|
// | column1 | column2 | column3 | ... |
// |---------|---------|---------|-----|
// columns are separated by semicolon, so column name can't contain semicolon
public class Schema implements Serializable {
  private Column[] columns_;
  private int size_;

  public Schema(Column[] columns) {
    columns_ = columns;
    size_ = 0;
    for (Column column : columns) {
      column.offset_ = size_;
      size_ += column.getMaxLength();
    }
  }

  // getters
  public Column[] getColumns() {
    return columns_;
  }

  public Column getColumn(String name) {
    for (Column column : columns_) {
      if (column.getName().equals(name)) {
        return column;
      }
    }
    return null;
  }

  public Column getColumn(int index) {
    if (index < 0 || index >= columns_.length) {
      throw new IndexOutOfBoundsException("getcolumn Index out of bound!");
    }
    return columns_[index];
  }

  public int getSize() {
    return size_;
  }

  public int getOffset(String name) {
    for (Column column : columns_) {
      if (column.getName().equals(name)) {
        return column.offset_;
      }
    }
    return -1;
  }

  // difference between toString and serialize:
  // toString is used for printing & debugging, and is human-readable
  // serialize is used for storage, it is compressed and can only decode by machine

  // toString
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("Schema:\n");
    for (Column column : columns_) {
      sb.append(column.toString());
      sb.append("'\n ");
    }
    return sb.toString();
  }

  public void serialize(ByteBuffer buffer, int offset) {
    for (Column column : columns_) {
      byte[] bytes = column.toString().getBytes();
      buffer.put(bytes, offset, bytes.length);
      offset += bytes.length;
    }
  }

  public static Schema deserialize(ByteBuffer buffer, int offset) {
    // TODO
    return null;
  }
}
