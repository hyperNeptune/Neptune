package cn.edu.thssdb.parser.tableBinder;

//
public class TableBinder {
  private final TableBinderType type;

  public TableBinder(TableBinderType type) {
    this.type = type;
  }

  public TableBinder() {
    this.type = TableBinderType.INVALID;
  }

  // for calculator
  public static TableBinder getEmptyTableBinder() {
    return new TableBinder(TableBinderType.EMPTY);
  }

  public TableBinderType getType() {
    return type;
  }
}
