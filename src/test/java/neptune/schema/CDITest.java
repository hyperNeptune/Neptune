package neptune.schema;

import neptune.backend.buffer.BufferPoolManager;
import neptune.backend.buffer.LRUReplacer;
import neptune.backend.buffer.ReplaceAlgorithm;
import neptune.backend.schema.*;
import neptune.backend.storage.DiskManager;
import neptune.backend.storage.Tuple;
import neptune.backend.type.*;
import neptune.common.Global;
import neptune.common.RID;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.file.Paths;

public class CDITest {
  private DiskManager diskManager;
  private BufferPoolManager bufferPoolManager;
  private ReplaceAlgorithm replaceAlgorithm;
  private Table table;
  private Schema sh;

  @Before
  public void setUp() throws Exception {
    replaceAlgorithm = new LRUReplacer(Global.DEFAULT_BUFFER_SIZE);
    diskManager = new DiskManager(Paths.get("test.db"));
    bufferPoolManager =
        new BufferPoolManager(Global.DEFAULT_BUFFER_SIZE, diskManager, replaceAlgorithm);
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

  @Test
  public void testCatalog() throws Exception {
    // 1. create a new cdi
    CimetiereDesInnocents cdi = CimetiereDesInnocents.createCDI(bufferPoolManager);
    // 2. create a catalog
    Catalog clg = cdi.createDatabase("test");
    // 3. create a table
    TableInfo ti = clg.createTable("test", sh);
    // 4. get the table
    Table t = clg.getTable("test");
    // 5. prepare and insert a tuple to table
    Value<?, ?>[] values = new Value[5];
    values[0] = new IntValue(1);
    values[1] = new DoubleValue(2.0);
    values[2] = new StringValue("issssssssss", 11);
    values[3] = new FloatValue(4.0f);
    values[4] = new LongValue(5);
    Tuple tuple = new Tuple(values, sh);
    // 6. insert the tuple
    RID id = new RID(0, 0);
    t.insert(tuple, id);
    // 7. flush the buffer pool
    bufferPoolManager.flushAllPages();
    // 8. open cdi
    cdi = CimetiereDesInnocents.openCDI(bufferPoolManager);
    // 9. get the catalog
    Catalog clg2 = cdi.useDatabase("test");
    // 10. get the table
    Table t2 = clg2.getTable("test");
    // 11. get the tuple
    Tuple tuple2 = t2.getTuple(id);
    // 12. check the tuple
    for (int i = 0; i < sh.getColumns().length; i++) {
      Assert.assertEquals(tuple.getValue(sh, i).getValue(), tuple2.getValue(sh, i).getValue());
    }
  }

  // after test, delete the test.db file
  @After
  public void tearDown() throws Exception {
    java.nio.file.Files.delete(Paths.get("test.db"));
  }
}
