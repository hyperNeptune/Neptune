package cn.edu.thssdb.parser.statement;

import cn.edu.thssdb.parser.expression.Expression;
import cn.edu.thssdb.schema.TableInfo;
import cn.edu.thssdb.utils.Pair;

public class UpdateStatement extends Statement {
  TableInfo tableInfo_;
  Pair<String, Expression> updateValue_;
  Expression where_;

  public UpdateStatement(
      TableInfo tableInfo, Pair<String, Expression> updateValue, Expression where) {
    super(StatementType.UPDATE);
    this.tableInfo_ = tableInfo;
    this.updateValue_ = updateValue;
    this.where_ = where;
  }

  public TableInfo getTable() {
    return tableInfo_;
  }

  public Pair<String, Expression> getUpdateValue() {
    return updateValue_;
  }

  public Expression getWhere() {
    return where_;
  }

  @Override
  public String toString() {
    return "statement: update";
  }
}
