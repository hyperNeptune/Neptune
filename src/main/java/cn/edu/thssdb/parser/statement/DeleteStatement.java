package cn.edu.thssdb.parser.statement;

import cn.edu.thssdb.parser.expression.Expression;
import cn.edu.thssdb.schema.TableInfo;

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
}
