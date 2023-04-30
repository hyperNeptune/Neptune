package cn.edu.thssdb.parser.statement;

import cn.edu.thssdb.schema.TableInfo;

public class ShowTbStatement extends Statement {
  private final TableInfo tableInfo;

  // constructor with 3 params
  public ShowTbStatement(TableInfo ti) {
    super(StatementType.SHOW_TABLES);
    this.tableInfo = ti;
  }

  // getter
  public TableInfo getTableInfo() {
    return tableInfo;
  }

  @Override
  public String toString() {
    return "Statement: Show Table" + "\n" + "Database Name: " + tableInfo.getTableName();
  }
}
