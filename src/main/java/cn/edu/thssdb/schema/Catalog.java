package cn.edu.thssdb.schema;

// this interface shows all table in our database
public interface Catalog {
  // read from disk
  public void load();
  // write to disk
  public void save();
  // list all
  public void list();
  // create a new table
  public void create(String tableName, Schema schema);
  // drop a table
  public void drop(String tableName);
  // get table info
  public TableInfo getTableInfo(String tableName);
  // get table schema
  public Schema getTableSchema(String tableName);
  // get table first page id
  public int getTableFirstPageId(String tableName);
  // set table first page id
  public void setTableFirstPageId(String tableName, int firstPageId);
}
