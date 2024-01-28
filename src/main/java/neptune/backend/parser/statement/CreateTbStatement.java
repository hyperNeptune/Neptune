package neptune.backend.parser.statement;

import neptune.backend.schema.Column;

public class CreateTbStatement extends Statement {
  private final Column[] columns;
  private final String tableName;

  public CreateTbStatement(String tableName, Column[] columns) {
    super(StatementType.CREATE_TABLE);
    this.tableName = tableName;
    this.columns = columns;
  }

  public Column[] getColumns() {
    return columns;
  }

  public String getTableName() {
    return tableName;
  }

  @Override
  public String toString() {
    return "Statement: Create Table " + tableName;
  }
}
