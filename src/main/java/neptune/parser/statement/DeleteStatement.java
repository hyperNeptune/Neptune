package neptune.parser.statement;

import neptune.parser.expression.*;
import neptune.parser.expression.Expression;
import neptune.schema.TableInfo;
import neptune.type.Value;

import static neptune.parser.statement.UpdateStatement.findKey;

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
