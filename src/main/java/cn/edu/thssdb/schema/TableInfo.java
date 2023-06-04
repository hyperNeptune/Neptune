package cn.edu.thssdb.schema;

import cn.edu.thssdb.buffer.BufferPoolManager;
import cn.edu.thssdb.storage.Tuple;
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
    buffer.position(offset);
    buffer.put(tableName_.getBytes(), 0, tableName_.length());
    buffer.clear();
    offset += tableName_.length();
    // schema
    schema_.serialize(buffer, offset);
    offset += schema_.getSchemaSize();
    // first_page_id
    buffer.putInt(offset, tableHeap_.getFirstPageId());
  }

  // serialize to Tuple
  public Tuple serialize() {
    ByteBuffer buffer = ByteBuffer.allocate(4 + tableName_.length() + schema_.getSchemaSize() + 4);
    serialize(buffer, 0);
    return new Tuple(buffer);
  }

  // static method: deserialize from ByteBuffer
  public static TableInfo deserialize(
      ByteBuffer buffer, int offset, BufferPoolManager bufferPoolManager) throws Exception {
    // table_name_length
    int tableNameLength = buffer.getInt(offset);
    offset += 4;
    // table_name
    byte[] tableNameBytes = new byte[tableNameLength];
    buffer.position(offset);
    buffer.get(tableNameBytes, 0, tableNameLength);
    buffer.clear();
    offset += tableNameLength;
    String tableName = new String(tableNameBytes);
    // schema
    Pair<Schema, Integer> dsr_result = Schema.deserialize(buffer, offset);
    Schema schema = dsr_result.left;
    offset = dsr_result.right;
    // first_page_id
    int firstPageId = buffer.getInt(offset);
    return new TableInfo(
        tableName, schema, SlotTable.openSlotTable(bufferPoolManager, firstPageId));
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

}