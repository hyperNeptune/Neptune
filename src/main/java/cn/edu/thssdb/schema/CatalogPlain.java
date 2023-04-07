package cn.edu.thssdb.schema;

// TODO: implement this class
public class CatalogPlain implements Catalog {
  @Override
  public void load() {}

  @Override
  public void save() {}

  @Override
  public void list() {}

  @Override
  public void create(String tableName, Schema schema) {}

  @Override
  public void drop(String tableName) {}

  @Override
  public TableInfo getTableInfo(String tableName) {
    return null;
  }

  @Override
  public Schema getTableSchema(String tableName) {
    return null;
  }

  @Override
  public int getTableFirstPageId(String tableName) {
    return 0;
  }

  @Override
  public void setTableFirstPageId(String tableName, int firstPageId) {}
}
