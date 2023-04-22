package cn.edu.thssdb.schema;

import cn.edu.thssdb.utils.Tuple2;

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
  private final int firstPageId_;

  public TableInfo(String tableName, Schema schema, int firstPageId) {
    tableName_ = tableName;
    schema_ = schema;
    firstPageId_ = firstPageId;
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
    buffer.putInt(offset, schema_.getSize());
    offset += 4;
    // schema
    schema_.serialize(buffer);
    offset += schema_.getSize();
    // first_page_id
    buffer.putInt(offset, firstPageId_);
  }

  // static method: deserialize from ByteBuffer
  public static TableInfo deserialize(ByteBuffer buffer, int offset) {
    // table_name_length
    int tableNameLength = buffer.getInt(offset);
    offset += 4;
    // table_name
    byte[] tableNameBytes = new byte[tableNameLength];
    buffer.get(tableNameBytes, offset, tableNameLength);
    offset += tableNameLength;
    String tableName = new String(tableNameBytes);
    // schema
    Tuple2<Schema, Integer> dsr_result = Schema.deserialize(buffer, offset);
    Schema schema = dsr_result.getFirst();
    offset = dsr_result.getSecond();
    // first_page_id
    int firstPageId = buffer.getInt(offset);
    return new TableInfo(tableName, schema, firstPageId);
  }

  // getters
  public String getTableName() {
    return tableName_;
  }

  public Schema getSchema() {
    return schema_;
  }

  public int getFirstPageId() {
    return firstPageId_;
  }
}
