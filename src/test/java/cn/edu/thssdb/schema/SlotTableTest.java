package cn.edu.thssdb.schema;

import cn.edu.thssdb.buffer.BufferPoolManager;
import cn.edu.thssdb.buffer.LRUReplacer;
import cn.edu.thssdb.buffer.ReplaceAlgorithm;
import cn.edu.thssdb.storage.DiskManager;
import cn.edu.thssdb.storage.Page;
import cn.edu.thssdb.storage.Tuple;
import cn.edu.thssdb.type.*;
import cn.edu.thssdb.utils.Global;
import cn.edu.thssdb.utils.Pair;
import cn.edu.thssdb.utils.RID;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.file.Paths;
import java.util.Iterator;

import static org.junit.Assert.assertEquals;

public class SlotTableTest {
  private DiskManager diskManager;
  private BufferPoolManager bufferPoolManager;
  private ReplaceAlgorithm replaceAlgorithm;
  private Page page;
  private Table table;

  @Before
  public void setUp() throws Exception {
    replaceAlgorithm = new LRUReplacer(Global.DEFAULT_BUFFER_SIZE);
    diskManager = new DiskManager(Paths.get("test.db"));
    bufferPoolManager =
        new BufferPoolManager(Global.DEFAULT_BUFFER_SIZE, diskManager, replaceAlgorithm);
    page = new Page(0);
  }

  @Test
  public void testTablePage() throws Exception {
    Column[] s = new Column[5];
    s[0] = new Column("col1", IntType.INSTANCE, (byte) 0, (byte) 0, Global.INT_SIZE, 0);
    // a double
    s[1] = new Column("col2", DoubleType.INSTANCE, (byte) 0, (byte) 0, Global.DOUBLE_SIZE, 4);
    // a string
    s[2] = new Column("col3", StringType.getVarCharType(10), (byte) 0, (byte) 0, 10, 12);
    // a float
    s[3] = new Column("col4", FloatType.INSTANCE, (byte) 0, (byte) 0, Global.FLOAT_SIZE, 22);
    // a long
    s[4] = new Column("col5", LongType.INSTANCE, (byte) 0, (byte) 0, Global.LONG_SIZE, 26);

    Schema sh = new Schema(s);
    table = SlotTable.newSlotTable(bufferPoolManager, sh.getTupleSize());

    Value<?, ?>[] v = new Value[5];
    v[0] = new IntValue(1);
    v[1] = new DoubleValue(2.0);
    v[2] = new StringValue("aaaaaaaaaa", 10);
    v[3] = new FloatValue(3.0f);
    v[4] = new LongValue(4);

    Tuple t = new Tuple(v, sh);
    System.out.println(sh);
    t.print(sh);

    v[0] = new IntValue(5);
    Tuple tt = new Tuple(v, sh);

    RID rid = new RID(0, 0);
    for (int i = 0; i < 1000; i++) {
      table.insert(t, rid);
    }
    System.out.println(rid.toString());
    table.update(tt, rid);
    table.getTuple(rid).print(sh);
    assertEquals(5, table.getTuple(rid).getValue(sh, 0).getValue());

    RID rid_delete = new RID(2, 14);
    table.delete(rid_delete);
    table.insert(tt, rid_delete);
    table.getTuple(rid_delete).print(sh);
    assertEquals(5, table.getTuple(rid_delete).getValue(sh, 0).getValue());
  }

  @Test
  public void testTableIterator() throws Exception {
    Column[] s = new Column[5];
    s[0] = new Column("col1", IntType.INSTANCE, (byte) 0, (byte) 0, Global.INT_SIZE, 0);
    // a double
    s[1] = new Column("col2", DoubleType.INSTANCE, (byte) 0, (byte) 0, Global.DOUBLE_SIZE, 4);
    // a string
    s[2] = new Column("col3", StringType.getVarCharType(10), (byte) 0, (byte) 0, 10, 12);
    // a float
    s[3] = new Column("col4", FloatType.INSTANCE, (byte) 0, (byte) 0, Global.FLOAT_SIZE, 22);
    // a long
    s[4] = new Column("col5", LongType.INSTANCE, (byte) 0, (byte) 0, Global.LONG_SIZE, 26);

    Schema sh = new Schema(s);
    table = SlotTable.newSlotTable(bufferPoolManager, sh.getTupleSize());

    Value<?, ?>[] v = new Value[5];
    v[0] = new IntValue(1);
    v[1] = new DoubleValue(2.0);
    v[2] = new StringValue("aaaaaaaaaa", 10);
    v[3] = new FloatValue(3.0f);
    v[4] = new LongValue(4);

    Tuple t = new Tuple(v, sh);
    System.out.println(sh);
    t.print(sh);

    v[0] = new IntValue(5);
    Tuple tt = new Tuple(v, sh);

    RID rid = new RID(0, 0);
    for (int i = 0; i < 1000; i++) {
      table.insert(t, rid);
    }
    System.out.println(rid.toString());
    table.update(tt, rid);
    table.getTuple(rid).print(sh);
    assertEquals(5, table.getTuple(rid).getValue(sh, 0).getValue());

    RID rid_delete = new RID(2, 14);
    table.delete(rid_delete);
    table.insert(tt, rid_delete);
    table.getTuple(rid_delete).print(sh);
    assertEquals(5, table.getTuple(rid_delete).getValue(sh, 0).getValue());

    // iterator
    Iterator<Pair<Tuple, RID>> iterator = table.iterator();
    int idx = 0;
    while (iterator.hasNext()) {
      Tuple tuple = iterator.next().left;
      idx++;
      tuple.print(sh);
      if (idx != 1000 && idx != 203) {
        // assertEquals(1, tuple.getValue(sh, 0).getValue());
      } else {
        System.out.println("special attention");
        // assertEquals(5, tuple.getValue(sh, 0).getValue());
      }
    }
  }

  // after test, delete the test.db file
  @After
  public void tearDown() throws Exception {
    java.nio.file.Files.delete(Paths.get("test.db"));
  }
}
