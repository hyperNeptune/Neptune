package neptune.backend.parser.statement;

import neptune.backend.parser.expression.*;
import neptune.backend.parser.expression.Expression;
import neptune.backend.schema.TableInfo;
import neptune.backend.type.Value;

import static neptune.backend.parser.statement.UpdateStatement.findKey;

public class DeleteStatement extends Statement {
  private final TableInfo table;
  private final Expression expression;

  public DeleteStatement(TableInfo table, Expression expression) {
    super(StatementType.DELETE);
    this.table = table;
    this.expression = expression;
  }

  public TableInfo getTable() {
    return table;
  }

  public Expression getExpression() {
    return expression;
  }

  @Override
  public String toString() {
    return "DeleteStatement{" + "table=" + table + ", expression=" + expression + '}';
  }

  public Value<?, ?> useIndex() {
    return findKey(expression, table);
  }
}
