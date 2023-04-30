package cn.edu.thssdb.parser.tableBinder;

import cn.edu.thssdb.schema.TableInfo;

public class RegularTableBinder extends TableBinder {
  private final TableInfo tableInfo;

  public RegularTableBinder(TableInfo tableInfo) {
    super(TableBinderType.REGULAR);
    this.tableInfo = tableInfo;
  }

  public TableInfo getTableInfo() {
    return tableInfo;
  }

  @Override
  public String toString() {
    return "RegularTableBinder{" + "tableInfo=" + tableInfo + '}';
  }
}
