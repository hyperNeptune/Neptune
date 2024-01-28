package neptune.backend.parser.statement;

public class DropTbStatement extends Statement {
  public final String tableName;

  public DropTbStatement(String tableName) {
    super(StatementType.DROP_TABLE);
    this.tableName = tableName;
  }

  @Override
  public StatementType getType() {
    return StatementType.DROP_TABLE;
  }
}
