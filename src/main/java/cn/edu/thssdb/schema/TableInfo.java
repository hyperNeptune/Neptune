package cn.edu.thssdb.schema;

import cn.edu.thssdb.buffer.BufferPoolManager;
import cn.edu.thssdb.utils.Pair;

import java.nio.ByteBuffer;

// Represents meta information about our table
// TableInfo aggregates to our database Catalog Table
// The Catalog Table is a table that stores information about all the tables in our database
// format:
// |-------------------|------------|--------|---------------|
// | table_name_length | table_name | schema | first_page_id |
// |-------------------|------------|--------|---------------|
// Schema is a serialized string of the table's schema
// Catalog is loaded into our database in bootstrap phase.
// This table will be the only variable length table in our database,
// and its rows are also variable length.
public class TableInfo {

  private final String tableName_;
  private final Schema schema_;
  private final Table tableHeap_;

  public TableInfo(String tableName, Schema schema, Table tableHeap) {
    tableName_ = tableName;
    schema_ = schema;
    tableHeap_ = tableHeap;
  }

  // serialize to ByteBuffer
  public void serialize(ByteBuffer buffer, int offset) {
    // table_name_length
    buffer.putInt(offset, tableName_.length());
    offset += 4;
    // table_name
    buffer.put(tableName_.getBytes(), offset, tableName_.length());
    offset += tableName_.length();
    // schema_length
    buffer.putInt(offset, schema_.getColNum());
    offset += 4;
    // schema
    schema_.serialize(buffer);
    offset += schema_.getColNum();
    // first_page_id
    buffer.putInt(offset, tableHeap_.getFirstPageId());
  }

  // static method: deserialize from ByteBuffer
  public static TableInfo deserialize(
      ByteBuffer buffer, int offset, BufferPoolManager bufferPoolManager) {
    // table_name_length
    int tableNameLength = buffer.getInt(offset);
    offset += 4;
    // table_name
    byte[] tableNameBytes = new byte[tableNameLength];
    buffer.get(tableNameBytes, offset, tableNameLength);
    offset += tableNameLength;
    String tableName = new String(tableNameBytes);
    // schema
    Pair<Schema, Integer> dsr_result = Schema.deserialize(buffer, offset);
    Schema schema = dsr_result.left;
    offset = dsr_result.right;
    // first_page_id
    int firstPageId = buffer.getInt(offset);
    return new TableInfo(tableName, schema, new Table(bufferPoolManager, firstPageId));
  }

  // getters
  public String getTableName() {
    return tableName_;
  }

  public Schema getSchema() {
    return schema_;
  }

  public Table getTable() {
    return tableHeap_;
  }

  public int getFirstPageId() {
    return tableHeap_.getFirstPageId();
  }
}
