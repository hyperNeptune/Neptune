package neptune.backend.parser.statement;

import neptune.backend.schema.TableInfo;

public class ShowTbStatement extends Statement {
  private final TableInfo tableInfo;

  // constructor with 3 params
  public ShowTbStatement(TableInfo ti) {
    super(StatementType.SHOW_TABLE);
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
