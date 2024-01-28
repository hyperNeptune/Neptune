package neptune.common;

import neptune.buffer.BufferPoolManager;
import neptune.buffer.LRUReplacer;
import neptune.buffer.ReplaceAlgorithm;
import neptune.concurrency.LockManager;
import neptune.concurrency.TransactionManager;
import neptune.execution.ExecutionEngine;
import neptune.schema.*;
import neptune.storage.DiskManager;
import neptune.storage.Tuple;
import neptune.type.*;
import neptune.utils.Global;

import java.nio.file.Paths;

public class TestUtility {
  public DiskManager diskManager;
  public BufferPoolManager bufferPoolManager;
  public ReplaceAlgorithm replaceAlgorithm;
  public CimetiereDesInnocents cdi;
  public Catalog curDB;
  public TableInfo mockTable;
  public TableInfo mockTable2;
  public TableInfo mockTable3;
  public Schema sh;
  public ExecutionEngine executionEngine;
  public TransactionManager transactionManager;
  public LockManager lockManager;

  public static final TestUtility INSTANCE = new TestUtility();

  public void setUp() throws Exception {
    replaceAlgorithm = new LRUReplacer(100);
    diskManager = new DiskManager(Paths.get("test.db"));
    bufferPoolManager = new BufferPoolManager(100, diskManager, replaceAlgorithm);
    cdi = CimetiereDesInnocents.createCDI(bufferPoolManager);
    curDB = cdi.createDatabase("test");
    executionEngine = new ExecutionEngine(curDB, transactionManager);
    lockManager = new LockManager();

    Column[] s = new Column[5];
    s[0] = new Column("col1", IntType.INSTANCE, (byte) 1, (byte) 0, Global.INT_SIZE, 0);
    // a double
    s[1] = new Column("col2", DoubleType.INSTANCE, (byte) 0, (byte) 0, Global.DOUBLE_SIZE, 4);
    // a string
    s[2] = new Column("col3", StringType.getVarCharType(10), (byte) 0, (byte) 0, 10, 12);
    // a float
    s[3] = new Column("col4", FloatType.INSTANCE, (byte) 0, (byte) 0, Global.FLOAT_SIZE, 22);
    // a long
    s[4] = new Column("col5", LongType.INSTANCE, (byte) 0, (byte) 0, Global.LONG_SIZE, 26);

    sh = new Schema(s);
  }

  // generate a mock table in curDB
  public void generateMockTable() throws Exception {
    curDB.createTable("test", sh);
    mockTable = curDB.getTableInfo("test");
    for (int i = 0; i < 1000; i++) {
      Value<?, ?>[] values = new Value[5];
      values[0] = new IntValue(i);
      values[1] = new DoubleValue(i);
      values[2] = new StringValue("test", 10);
      values[3] = new FloatValue(i);
      values[4] = new LongValue(i);
      Tuple t = new Tuple(values, sh);
      mockTable.getTable().insert(t, null);
    }
  }

  public void generateMockTable2() throws Exception {
    Column[] s = new Column[5];
    s[0] = new Column("col11", IntType.INSTANCE, (byte) 1, (byte) 0, Global.INT_SIZE, 0);
    // a double
    s[1] = new Column("col21", DoubleType.INSTANCE, (byte) 0, (byte) 0, Global.DOUBLE_SIZE, 4);
    // a string
    s[2] = new Column("col31", StringType.getVarCharType(10), (byte) 0, (byte) 0, 10, 12);
    // a float
    s[3] = new Column("col41", FloatType.INSTANCE, (byte) 0, (byte) 0, Global.FLOAT_SIZE, 22);
    // a long
    s[4] = new Column("col51", LongType.INSTANCE, (byte) 0, (byte) 0, Global.LONG_SIZE, 26);

    Schema shh = new Schema(s);
    curDB.createTable("test2", shh);
    mockTable2 = curDB.getTableInfo("test2");
    for (int i = 0; i < 1000; i++) {
      Value<?, ?>[] values = new Value[5];
      values[0] = new IntValue(i);
      values[1] = new DoubleValue(i);
      values[2] = new StringValue("testthis...", 10);
      values[3] = new FloatValue(i);
      values[4] = new LongValue(i);
      Tuple t = new Tuple(values, shh);
      mockTable2.getTable().insert(t, null);
    }
  }

  public void generateMockTable3() throws Exception {
    Column[] s = new Column[5];
    s[0] = new Column("col12", IntType.INSTANCE, (byte) 1, (byte) 0, Global.INT_SIZE, 0);
    // a double
    s[1] = new Column("col22", DoubleType.INSTANCE, (byte) 0, (byte) 0, Global.DOUBLE_SIZE, 4);
    // a string
    s[2] = new Column("col32", StringType.getVarCharType(10), (byte) 0, (byte) 0, 10, 12);
    // a float
    s[3] = new Column("col42", FloatType.INSTANCE, (byte) 0, (byte) 0, Global.FLOAT_SIZE, 22);
    // a long
    s[4] = new Column("col52", LongType.INSTANCE, (byte) 0, (byte) 0, Global.LONG_SIZE, 26);

    Schema shh = new Schema(s);
    curDB.createTable("test3", shh);
    mockTable3 = curDB.getTableInfo("test3");
    for (int i = 0; i < 1000; i++) {
      Value<?, ?>[] values = new Value[5];
      values[0] = new IntValue(i);
      values[1] = new DoubleValue(i);
      values[2] = new StringValue("testthis...", 10);
      values[3] = new FloatValue(i);
      values[4] = new LongValue(i);
      Tuple t = new Tuple(values, shh);
      mockTable3.getTable().insert(t, null);
    }
  }

  public void tearDown() throws Exception {
    java.nio.file.Files.deleteIfExists(Paths.get("test.db"));
  }
}
