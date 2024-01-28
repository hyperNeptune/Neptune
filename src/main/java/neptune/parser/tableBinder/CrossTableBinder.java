package neptune.parser.tableBinder;

public class CrossTableBinder extends TableBinder {
  private final TableBinder left;
  private final TableBinder right;

  public CrossTableBinder(TableBinder left, TableBinder right) {
    super(TableBinderType.CROSS);
    this.left = left;
    this.right = right;
  }

  public TableBinder getLeft() {
    return left;
  }

  public TableBinder getRight() {
    return right;
  }

  @Override
  public String toString() {
    return "CrossTableBinder{}";
  }
}
