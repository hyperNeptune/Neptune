package cn.edu.thssdb.execution;

import cn.edu.thssdb.execution.executor.Executor;
import cn.edu.thssdb.parser.statement.*;
import cn.edu.thssdb.parser.tableBinder.TableBinder;
import cn.edu.thssdb.parser.tableBinder.TableBinderType;
import cn.edu.thssdb.schema.Catalog;

public class Planner {
  private final Catalog catalog_;
  private Executor plan_;

  public Planner(Catalog catalog) {
    catalog_ = catalog;
  }

  public Executor plan(Statement stmt) {
    switch (stmt.getType()) {
      case SELECT:
        plan_ = planSelect((SelectStatement) stmt);
        break;
      case INSERT:
        plan_ = planInsert((InsertStatement)stmt);
        break;
      case DELETE:
        plan_ = planDelete((DeleteStatement)stmt);
        break;
      case UPDATE:
        plan_ = planUpdate((UpdateStatement)stmt);
        break;
      default:
        throw new RuntimeException("unreachable. Unsupported statement: " + stmt.getType());
    }
    return plan_;
  }

  private Executor planUpdate(UpdateStatement stmt) {
    return null;
  }

  private Executor planDelete(DeleteStatement stmt) {
    return null;
  }

  private Executor planInsert(InsertStatement stmt) {
    return null;
  }

  private Executor planSelect(SelectStatement stmt) {
    Executor plan = null;
    // plan FROM
    TableBinder tab = stmt.getFrom();
    if (tab.getType() == TableBinderType.EMPTY) {
      // an empty table binder means a calculator database
    } else {
      plan = planTable(tab);
    }
    return null;
  }

  private Executor planTable(TableBinder tab) {
    return null;
  }

  public Executor getPlan() {
    return plan_;
  }
}
