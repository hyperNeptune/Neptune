package cn.edu.thssdb.schema;

import cn.edu.thssdb.buffer.BufferPoolManager;
import cn.edu.thssdb.buffer.LRUReplacer;
import cn.edu.thssdb.buffer.ReplaceAlgorithm;
import cn.edu.thssdb.storage.DiskManager;
import cn.edu.thssdb.storage.Tuple;
import cn.edu.thssdb.type.*;
import cn.edu.thssdb.utils.Global;
import cn.edu.thssdb.utils.RID;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.file.Paths;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class CatalogTest {
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
    // 1. create a catalog
    Catalog clg = Catalog.createCatalog(bufferPoolManager);
    // 2. create a table
    TableInfo ti = clg.createTable("test", sh);
    // 3. get the table
    Table t = clg.getTable("test");
    // 4. prepare and insert a tuple to table
    Value[] values = new Value[5];
    values[0] = new IntValue(1);
    values[1] = new DoubleValue(2.0);
    values[2] = new StringValue("a great string", 10);
    values[3] = new FloatValue(4.0f);
    values[4] = new LongValue(5L);
    Tuple tuple = new Tuple(values, sh);
    // 5. insert the tuple
    RID id = new RID(0, 0);
    t.insert(tuple, id);
    // 6. flush buffer pool
    bufferPoolManager.flushAllPages();
    // 7. load the catalog from disk
    Catalog clg2 = Catalog.loadCatalog(bufferPoolManager, clg.getFirstPageId());
    // 8. compare the content of clg.list(), print them
    String[] names = clg.list();
    String[] names2 = clg2.list();
    assertEquals(names.length, names2.length);
    for (int i = 0; i < names.length; i++) {
      assertEquals(names[i], names2[i]);
      System.out.println(names[i]);
    }
    // 9. get the table from clg2
    Table t2 = clg2.getTable("test");
    // 10. fetch RID tuple
    Tuple tuple2 = t2.getTuple(id);
    // 11. compare the content of tuple and tuple2. use sh
    for (int i = 0; i < sh.getColumns().length; i++) {
      assertEquals(tuple.getValue(sh, i).getValue(), tuple2.getValue(sh, i).getValue());
    }
    // 12. drop the table
    clg.dropTable("test");
    // 13. check if the table is dropped
    assertNull(clg.getTable("test"));
  }

  // after test, delete the test.db file
  @After
  public void tearDown() throws Exception {
    java.nio.file.Files.delete(Paths.get("test.db"));
  }
}
