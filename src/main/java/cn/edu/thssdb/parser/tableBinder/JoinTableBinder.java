package cn.edu.thssdb.parser.tableBinder;

import cn.edu.thssdb.parser.expression.Expression;

public class JoinTableBinder extends TableBinder {
  private final TableBinder left;
  private final TableBinder right;
  private final Expression on;

  public JoinTableBinder(TableBinder left, TableBinder right, Expression on) {
    super(TableBinderType.JOIN);
    this.left = left;
    this.right = right;
    this.on = on;
  }

  @Override
  public String toString() {
    return "JoinTableBinder{}";
  }
}
