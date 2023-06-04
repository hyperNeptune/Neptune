package cn.edu.thssdb.storage;

import cn.edu.thssdb.schema.Column;
import cn.edu.thssdb.schema.Schema;
import cn.edu.thssdb.type.*;

import java.nio.ByteBuffer;

// tuple
// | bitmap size | bitmap | data size | true data |
public class Tuple {
  // invariant: size_ = data_.capacity() = dataSize_ + bitmapSize_ + FIX_HDR_SIZE
  private int size_;
  private ByteBuffer data_;
  int bitmapSize_;
  byte[] bitmap_;
  int dataSize_;
  public static final int FIX_HDR_SIZE = 8;

  // default constructor
  public Tuple() {
    size_ = 0;
    data_ = null;
  }

  // constructor by another tuple
  // shallow
  public Tuple(Tuple tuple) {
    size_ = tuple.size_;
    data_ = tuple.data_;
    bitmapSize_ = tuple.bitmapSize_;
    bitmap_ = tuple.bitmap_;
    dataSize_ = tuple.dataSize_;
  }

  // constructor by values and schema
  public Tuple(Value<?, ?>[] values, Schema schema) {
    // 1. we calculate true size of tuple
    bitmapSize_ = (schema.getColNum() + 7) / 8;
    bitmap_ = new byte[bitmapSize_];
    dataSize_ = schema.getDataSize();
    size_ = bitmapSize_ + dataSize_ + FIX_HDR_SIZE;
    data_ = ByteBuffer.allocate(size_);

    // 2. we set bitmap according to values
    for (int i = 0; i < values.length; i++) {
      if (values[i].isNull()) {
        bitmap_[i / 8] |= (1 << (i % 8));
      }
    }

    // 3. we start serialize
    data_.putInt(0, bitmapSize_);
    ((ByteBuffer) data_.position(4)).put(bitmap_, 0, bitmapSize_).clear();
    data_.putInt(bitmapSize_ + 4, dataSize_);
    for (int i = 0; i < values.length; i++) {
      values[i].serialize(data_, bitmapSize_ + FIX_HDR_SIZE + schema.getColumn(i).getOffset());
    }
  }

  // constructor by raw data.
  public Tuple(ByteBuffer data) {
    size_ = data.capacity();
    data_ = data;
  }

  // constructor by sole schema
  // writes the schema into the tuple
  public Tuple(Schema schema) {
    ByteBuffer data = ByteBuffer.allocate(schema.getSchemaSize());
    schema.serialize(data, 0);
    size_ = data.capacity();
    data_ = data;
  }

  public ByteBuffer getValue() {
    return data_;
  }

  // get value by schema and column index
  public Value<?, ?> getValue(Schema schema, int index) {
    Column column = schema.getColumn(index);
    if ((bitmap_[index / 8] & (1 << (index % 8))) != 0) {
      return column.getType().getNullValue();
    }
    return column
        .getType()
        .deserializeValue(data_, bitmapSize_ + FIX_HDR_SIZE + column.getOffset());
  }

  // serialize
  public void serialize(ByteBuffer buffer, int offset) {
    buffer.position(offset);
    buffer.put(data_.array(), 0, size_).clear();
  }

  // deserialize by schema
  public static Tuple deserialize(ByteBuffer buffer, int offset) {
    Tuple tuple = new Tuple();
    tuple.bitmapSize_ = buffer.getInt(offset);
    tuple.bitmap_ = new byte[tuple.bitmapSize_];
    ((ByteBuffer) buffer.position(offset + 4)).get(tuple.bitmap_, 0, tuple.bitmapSize_).clear();
    tuple.dataSize_ = buffer.getInt(offset + 4 + tuple.bitmapSize_);
    tuple.size_ = tuple.bitmapSize_ + tuple.dataSize_ + FIX_HDR_SIZE;
    tuple.data_ = ByteBuffer.allocate(tuple.size_);
    tuple.data_.put(buffer.array(), offset, tuple.size_);
    return tuple;
  }

  // deserialize by size
  public static Tuple deserialize(ByteBuffer buffer, int offset, int size) {
    Tuple tuple = new Tuple();
    tuple.size_ = size;
    tuple.data_ = ByteBuffer.allocate(tuple.size_);
    tuple.data_.put(buffer.array(), offset, tuple.size_);
    return tuple;
  }

  public void print(Schema schema) {
    System.out.println(toString(schema));
  }

  // like print, but return a string
  public String toString(Schema sh) {
    StringBuilder s = new StringBuilder("Tuple: |");
    for (int i = 0; i < sh.getColNum(); i++) {
      s.append(getValue(sh, i).toString()).append("|");
    }
    return s.toString();
  }

  // toString
  @Override
  public String toString() {
    // byte array to String
    return new String(data_.array());
  }

  // get size
  public int getSize() {
    return size_;
  }

  // copy assign
  public void copyAssign(Tuple tuple) {
    size_ = tuple.size_;
    data_ = tuple.data_;
    bitmapSize_ = tuple.bitmapSize_;
    bitmap_ = tuple.bitmap_;
    dataSize_ = tuple.dataSize_;
  }
}
