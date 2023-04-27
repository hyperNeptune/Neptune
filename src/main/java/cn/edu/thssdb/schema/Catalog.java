package cn.edu.thssdb.schema;

// TODO: implement this class
public class Catalog {
  public void load() {}

  public void save() {}

  public void list() {}

  public void create(String tableName, Schema schema) {}

  public void drop(String tableName) {}

  public TableInfo getTableInfo(String tableName) {
    return null;
  }

  public Schema getTableSchema(String tableName) {
    return null;
  }

  public int getTableFirstPageId(String tableName) {
    return 0;
  }

  public void setTableFirstPageId(String tableName, int firstPageId) {}
}
