package cn.edu.thssdb.storage;

import java.nio.ByteBuffer;

import cn.edu.thssdb.schema.Column;
import cn.edu.thssdb.schema.Schema;
import cn.edu.thssdb.type.*;
import cn.edu.thssdb.utils.RID;

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
    data_ = ByteBuffer.allocate(size_).clear();

    for (int i = 0; i < values.length; i++) {
      values[i].serialize(data_, schema.getColumn(i).getOffset());
    }
  }

  // get value by schema and column index
  public Value getValue(Schema schema, int index) {
    Column column = schema.getColumn(index);
    return Value.deserialize(data_, column.getOffset(), column.getType());
  }

  // serialize
  public void serialize(ByteBuffer buffer, int offset) {
    buffer.rewind()
          .put(data_.array(), 0, size_)
          .rewind();
  }

  // deserialize
  public static Tuple deserialize(ByteBuffer buffer, int offset, Schema schema) {
    Tuple tuple = new Tuple();
    tuple.size_ = schema.getSize();
    tuple.data_ = ByteBuffer.allocate(tuple.size_).clear();
    tuple.data_.put(buffer.array(), offset, tuple.size_).rewind();
    return tuple;
  }

}
