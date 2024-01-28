package neptune.benchmark.generator;

import neptune.benchmark.common.Constants;
import neptune.benchmark.common.DataType;
import neptune.benchmark.common.TableSchema;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class PerformanceDataGenerator extends BaseDataGenerator {

  private int tableNum;
  private int columnNum = 10;
  private Random random = new Random(666);

  public PerformanceDataGenerator(int tableNum) {
    this.tableNum = tableNum;
    initTableSchema();
  }

  @Override
  protected void initTableSchema() {
    for (int tableId = 0; tableId < tableNum; tableId++) {
      String tableName = "test_table" + tableId;
      List<String> columns = new ArrayList<>();
      List<DataType> types = new ArrayList<>();
      List<Boolean> notNull = new ArrayList<>();
      columns.add("id");
      types.add(DataType.INT);
      notNull.add(true);
      for (int columnId = 1; columnId < columnNum; columnId++) {
        columns.add("column" + columnId);
        types.add(Constants.columnTypes[columnId % Constants.columnTypes.length]);
        notNull.add(false);
      }
      schemaMap.put(tableName, new TableSchema(tableName, columns, types, notNull, 0));
    }
  }

  @Override
  public Object generateValue(String tableName, int rowId, int columnId) {
    if (columnId == 0) {
      return rowId;
    }
    switch (schemaMap.get(tableName).types.get(columnId)) {
      case INT:
        return Math.abs(random.nextInt());
      case LONG:
        return Math.abs(random.nextLong());
      case DOUBLE:
        return Math.abs(random.nextDouble());
      case FLOAT:
        return Math.abs(random.nextFloat());
      case STRING:
        return String.format(stringFormat, rowId);
    }
    return null;
  }
}
