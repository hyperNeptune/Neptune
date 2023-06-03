package cn.edu.thssdb.schema;

import cn.edu.thssdb.storage.Tuple;
import cn.edu.thssdb.utils.Pair;

import java.io.Serializable;
import java.nio.ByteBuffer;

// assume all columns are inlined
// schema is a list of columns
// |------|---------|---------|---------|-----|
// |colNum| column1 | column2 | column3 | ... |
// |------|---------|---------|---------|-----|
// columns are separated by semicolon, so column name can't contain semicolon
public class Schema implements Serializable {
  private final Column[] columns_;
  private final int colNum;
  private final int dataSize_;
  private int schemaSize_ = 4;

  public Schema(Column[] columns) {
    columns_ = columns;
    colNum = columns_.length;
    int offset = 0;
    for (Column column : columns) {
      column.offset_ = offset;
      offset += column.getMaxLength();
      schemaSize_ += column.getColMetaSize();
    }
    dataSize_ = offset;
  }

  public Schema(Column[] columns, String tableName) {
    columns_ = columns;
    colNum = columns_.length;
    int offset = 0;
    for (Column column : columns) {
      column.offset_ = offset;
      offset += column.getMaxLength();
      schemaSize_ += column.getColMetaSize();
    }
    dataSize_ = offset;
    for (Column column : columns) {
      column.setFullNameByTable(tableName);
    }
  }

  // getters
  public Column[] getColumns() {
    return columns_;
  }

  public Column getColumn(String name) {
    if (name.contains(".")) {
      // full name search
      for (Column column : columns_) {
        if (column.getFullname().equals(name)) {
          return column;
        }
      }
    } else {
      // name search
      for (Column column : columns_) {
        if (column.getName().equals(name)) {
          return column;
        }
      }
    }
    return null;
  }

  public Column getColumn(int index) {
    if (index < 0 || index >= columns_.length) {
      throw new IndexOutOfBoundsException("getColumn Index out of bound!");
    }
    return columns_[index];
  }

  public int getColNum() {
    return colNum;
  }

  public int getDataSize() {
    return dataSize_;
  }

  // tuple size is bigger than data size
  // because of the header
  public int getTupleSize() {
    return dataSize_ + Tuple.FIX_HDR_SIZE + (colNum + 7) / 8;
  }

  public int getSchemaSize() {
    return schemaSize_;
  }

  // get column index by its name
  public int getColumnOrder(String name) {
    if (name.contains(".")) {
      // full name search
      for (int i = 0; i < columns_.length; i++) {
        if (columns_[i].getFullname().equals(name)) {
          return i;
        }
      }
    } else {
      // name search
      for (int i = 0; i < columns_.length; i++) {
        if (columns_[i].getName().equals(name)) {
          return i;
        }
      }
    }
    return -1;
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

  public int serialize(ByteBuffer buffer, int offset) {
    buffer.putInt(offset, colNum);
    offset += 4;
    for (Column column : columns_) {
      offset = column.serialize(buffer, offset);
      buffer.put(offset++, (byte) ';');
    }
    return offset;
  }

  // WARNING: offset is changed after calling this function
  public static Pair<Schema, Integer> deserialize(ByteBuffer buffer, Integer offset, String tableName) {
    // column one by one until end
    // column format: name,type,primary nullable maxLength offset
    // not all separated by comma
    int coln = buffer.getInt(offset);
    offset += 4;
    Column[] columns = new Column[coln];
    for (int i = 0; i < coln; i++) {
      Pair<Column, Integer> dsr_result = Column.deserialize(buffer, offset);
      columns[i] = dsr_result.left;
      if (tableName != null) {
        columns[i].setFullNameByTable(tableName);
      }
      offset = dsr_result.right;
    }
    return new Pair<>(new Schema(columns), offset);
  }

  // for compatibility with old apis
  public static Pair<Schema, Integer> deserialize(ByteBuffer buffer, Integer offset) {
    return deserialize(buffer, offset, null);
  }

}
