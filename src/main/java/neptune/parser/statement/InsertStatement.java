package neptune.parser.statement;

import neptune.schema.TableInfo;
import neptune.storage.Tuple;

public class InsertStatement extends Statement {
  private final TableInfo table_;
  private final Tuple[] tuple_;

  // constructor with 2 params
  public InsertStatement(TableInfo ti, Tuple[] tuple) {
    super(StatementType.INSERT);
    this.table_ = ti;
    this.tuple_ = tuple;
  }

  // getter
  public TableInfo getTable() {
    return table_;
  }

  public Tuple[] getTuple() {
    return tuple_;
  }

  @Override
  public String toString() {
    return "Statement: Insert" + "\n" + "Table Name: " + table_.getTableName();
  }
}
