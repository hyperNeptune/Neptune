package cn.edu.thssdb.storage;

import cn.edu.thssdb.schema.Column;
import cn.edu.thssdb.schema.Schema;
import cn.edu.thssdb.type.*;
import cn.edu.thssdb.utils.RID;

import java.nio.ByteBuffer;

// Fixed-length tuple
public class Tuple {
  private ByteBuffer data_;
  private int size_;
  // Record ID, the identifier of a tuple
  RID rid_;

  // default constructor
  public Tuple() {
    size_ = 0;
    data_ = null;
    rid_ = null;
  }

  // constructor by values and schema
  public Tuple(Value[] values, Schema schema) {
    size_ = schema.getSize();
    data_ = ByteBuffer.allocate(size_);

    for (int i = 0; i < values.length; i++) {
      values[i].serialize(data_, schema.getColumn(i).getOffset());
    }
  }

  // get value by schema and column index
  public Value getValue(Schema schema, int index) {
    Column column = schema.getColumn(index);
    return column.getType().deserializeValue(data_, column.getOffset());
  }

  // serialize
  public void serialize(ByteBuffer buffer, int offset) {
    buffer.position(offset);
    buffer.put(data_.array(), 0, size_).clear();
  }

  // deserialize
  public static Tuple deserialize(ByteBuffer buffer, int offset, Schema schema) {
    Tuple tuple = new Tuple();
    tuple.size_ = schema.getSize();
    tuple.data_ = ByteBuffer.allocate(tuple.size_);
    tuple.data_.put(buffer.array(), offset, tuple.size_);
    return tuple;
  }

  public void print(Schema schema) {
    System.out.println(toString(schema));
  }

  // like print, but return a string
  public String toString(Schema sh) {
    String s = "Tuple: |";
    for (int i = 0; i < sh.getColNum(); i++) {
      s += getValue(sh, i).toString() + "|";
    }
    return s;
  }
}
