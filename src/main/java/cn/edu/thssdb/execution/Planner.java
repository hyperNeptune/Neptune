package cn.edu.thssdb.execution;

import cn.edu.thssdb.execution.executor.*;
import cn.edu.thssdb.parser.expression.ConstantExpression;
import cn.edu.thssdb.parser.statement.*;
import cn.edu.thssdb.parser.tableBinder.*;
import cn.edu.thssdb.schema.Catalog;
import cn.edu.thssdb.schema.TableInfo;
import cn.edu.thssdb.type.BoolValue;

public class Planner {
  private final Catalog catalog_;
  private Executor plan_;
  private ExecContext ctx_;

  public Planner(Catalog catalog, ExecContext execContext) {
    catalog_ = catalog;
    ctx_ = execContext;
  }

  public Executor plan(Statement stmt) {
    switch (stmt.getType()) {
      case SELECT:
        plan_ = planSelect((SelectStatement) stmt);
        break;
      case INSERT:
        plan_ = planInsert((InsertStatement) stmt);
        break;
      case DELETE:
        plan_ = planDelete((DeleteStatement) stmt);
        break;
      case UPDATE:
        plan_ = planUpdate((UpdateStatement) stmt);
        break;
      default:
        throw new RuntimeException("unreachable. Unsupported statement: " + stmt.getType());
    }
    return plan_;
  }

  private Executor planUpdate(UpdateStatement stmt) {
    // plan FROM
    TableInfo tab = stmt.getTable();
    if (tab == null) {
      throw new RuntimeException("unreachable. table should not be null");
    }
    plan_ = new SeqScanExecutor(tab, ctx_);
    // plan WHERE
    if (stmt.getWhere() != null) {
      plan_ = new filterExecutor(plan_, stmt.getWhere(), ctx_);
    }
    // plan SET
    return new UpdateExecutor(ctx_, plan_, stmt.getUpdateValue(), tab);
  }

  private Executor planDelete(DeleteStatement stmt) {
    // plan FROM
    TableInfo tab = stmt.getTable();
    if (tab == null) {
      throw new RuntimeException("unreachable. table should not be null");
    }
    plan_ = new SeqScanExecutor(tab, ctx_);
    // plan WHERE
    if (stmt.getExpression() != null) {
      plan_ = new filterExecutor(plan_, stmt.getExpression(), ctx_);
    }
    // plan DELETE
    return plan_ = new deleteExecutor(ctx_, plan_, tab);
  }

  private Executor planInsert(InsertStatement stmt) {
    return plan_ = new InsertExecutor(ctx_, stmt.getTable(), stmt.getTuple());
  }

  private Executor planSelect(SelectStatement stmt) {
    Executor plan = null;
    // plan FROM
    TableBinder tab = stmt.getFrom();
    if (tab != null && tab.getType() != TableBinderType.EMPTY) {
      // an empty table binder means a calculator database
      plan = planTable(tab);
    }
    // plan WHERE
    if (stmt.getWhere() != null) {
      plan = new filterExecutor(plan, stmt.getWhere(), ctx_);
    }
    // plan SELECT
    if (stmt.getSelectList() == null) {
      throw new RuntimeException("unreachable. select list should not be null");
    }
    plan = new projectionExecutor(ctx_, plan, stmt.getSelectList());
    return plan_ = plan;
  }

  private Executor planTable(TableBinder tab) {
    switch (tab.getType()) {
      case REGULAR:
        return planRegularTable((RegularTableBinder) tab);
      case JOIN:
        return planJoinTable((JoinTableBinder) tab);
      case CROSS:
        return planCrossTable((CrossTableBinder) tab);
      case EMPTY:
        return null;
      default:
        break;
    }
    throw new RuntimeException("unreachable. Unsupported table binder type: " + tab.getType());
  }

  private Executor planCrossTable(CrossTableBinder tab) {
    Executor cross_left = planTable(tab.getLeft());
    Executor cross_right = planTable(tab.getRight());
    return new nestedLoopJoinExecutor(
        cross_left, cross_right, new ConstantExpression(BoolValue.ALWAYS_TRUE), ctx_);
  }

  private Executor planJoinTable(JoinTableBinder tab) {
    Executor join_left = planTable(tab.getLeft());
    Executor join_right = planTable(tab.getRight());
    return new nestedLoopJoinExecutor(join_left, join_right, tab.getOn(), ctx_);
  }

  private Executor planRegularTable(RegularTableBinder tab) {
    return new SeqScanExecutor(tab.getTableInfo(), ctx_);
  }

  public Executor getPlan() {
    return plan_;
  }
}
