package cn.edu.thssdb.schema;

import cn.edu.thssdb.storage.Tuple;
import cn.edu.thssdb.utils.Pair;

import java.io.Serializable;
import java.nio.ByteBuffer;

// assume all columns are inlined
// schema is a list of columns
// |------|---------|---------|---------|-----|
// | size | column1 | column2 | column3 | ... |
// |------|---------|---------|---------|-----|
// columns are separated by semicolon, so column name can't contain semicolon
public class Schema implements Serializable {
  private final Column[] columns_;
  private final int colNum;
  private final int size;

  public Schema(Column[] columns) {
    columns_ = columns;
    colNum = columns_.length;
    int offset = 0;
    for (Column column : columns) {
      column.offset_ = offset;
      offset += column.getMaxLength();
    }
    size = offset;
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

  public int getColNum() {
    return colNum;
  }

  public int getDataSize() {
    return size;
  }

  // tuple size is bigger than data size
  // because of the header
  public int getTupleSize() {
    return size + Tuple.FIX_HDR_SIZE + (colNum + 7) / 8;
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

  public void serialize(ByteBuffer buffer) {
    // size
    buffer.putInt(colNum);
    for (Column column : columns_) {
      column.serialize(buffer);
      buffer.put((byte) ';');
    }
  }

  // WARNING: offset is changed after calling this function
  public static Pair<Schema, Integer> deserialize(ByteBuffer buffer, Integer offset) {
    // column one by one until end
    // column format: name,type,primary,nullable,maxLength,offset
    // separated by comma
    int size = buffer.getInt(offset);
    offset += 4;
    Column[] columns = new Column[size];
    for (int i = 0; i < size; i++) {
      Pair<Column, Integer> dsr_result = Column.deserialize(buffer, offset);
      columns[i] = dsr_result.left;
      offset = dsr_result.right;
    }
    return new Pair<>(new Schema(columns), offset);
  }
}
