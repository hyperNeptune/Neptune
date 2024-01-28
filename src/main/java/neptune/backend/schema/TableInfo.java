package neptune.backend.schema;

import neptune.backend.buffer.BufferPoolManager;
import neptune.backend.storage.Tuple;
import neptune.backend.storage.index.BPlusTree;
import neptune.common.Global;
import neptune.common.Pair;

import java.nio.ByteBuffer;

// Represents meta information about our table
// TableInfo aggregates to our database Catalog Table
// The Catalog Table is a table that stores information about all the tables in our database
// format:
// |-------------------|------------|--------|---------------|-----------------------|
// | table_name_length | table_name | schema | first_page_id | pk index first page id|
// |-------------------|------------|--------|---------------|-----------------------|
// Schema is a serialized string of the table's schema
// Catalog is loaded into our database in bootstrap phase.
// This table will be the only variable length table in our database,
// and its rows are also variable length.
public class TableInfo {

  private final String tableName_;
  private final Schema schema_;
  private final Table tableHeap_;
  private final BPlusTree primaryIndex_;

  public TableInfo(String tableName, Schema schema, Table tableHeap, BPlusTree bpt) {
    tableName_ = tableName;
    schema_ = schema;
    tableHeap_ = tableHeap;
    primaryIndex_ = bpt;
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
    offset += 4;
    // pk index first page id
    if (primaryIndex_ != null) {
      buffer.putInt(offset, primaryIndex_.getRootPageId());
    } else {
      buffer.putInt(offset, Global.PAGE_ID_INVALID);
    }
  }

  // serialize to Tuple
  public Tuple serialize() {
    ByteBuffer buffer =
        ByteBuffer.allocate(4 + tableName_.length() + schema_.getSchemaSize() + 4 + 4);
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
    offset += 4;
    // pk index first page id
    int pkIndexFirstPageId = buffer.getInt(offset);
    // find pk
    Column pkColumn = schema.getPkColumn();
    if (pkColumn == null) {
      throw new Exception("Primary key not found");
    }
    return new TableInfo(
        tableName,
        schema,
        SlotTable.openSlotTable(bufferPoolManager, firstPageId),
        new BPlusTree(pkIndexFirstPageId, bufferPoolManager, pkColumn.getType()));
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

  public BPlusTree getIndex() {
    return primaryIndex_;
  }

  public Column getPKColumn() {
    return schema_.getPkColumn();
  }
}
