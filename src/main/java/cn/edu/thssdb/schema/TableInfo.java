package cn.edu.thssdb.schema;

// Represents meta information about our table
// TableInfo aggregates to our database Catalog Table
// The Catalog Table is a table that stores information about all the tables in our database
// format:
// |-------------------|------------|---------------|--------|---------------|
// | table_name_length | table_name | schema_length | schema | first_page_id |
// |-------------------|------------|---------------|--------|---------------|
// Schema is a serialized string of the table's schema
// Catalog is loaded into our database in bootstrap phase.
// This table will be the only variable length table in our database,
// and its rows are also variable length.
public class TableInfo {

  private final String tableName_;
  private Schema schema_;
  private int firstPageId_;

  public TableInfo(String tableName, Schema schema) {
    tableName_ = tableName;
    schema_ = schema;
  }

  int columnFind(String name) {
    // TODO
    return 0;
  }
}
