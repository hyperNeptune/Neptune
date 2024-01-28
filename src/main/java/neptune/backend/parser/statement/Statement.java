package neptune.backend.parser.statement;

// Statement is the base class for all statements
public abstract class Statement {
  private final StatementType type;

  public Statement(StatementType type) {
    this.type = type;
  }

  public Statement() {
    this.type = StatementType.INVALID;
  }

  public StatementType getType() {
    return type;
  }

  @Override
  public String toString() {
    return "You are not expecting this.";
  }
}
