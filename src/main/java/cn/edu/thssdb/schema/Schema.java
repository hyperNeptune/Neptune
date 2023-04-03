package cn.edu.thssdb.schema;

import java.io.Serializable;

// assume all columns are inlined
public class Schema implements Serializable{
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

}
