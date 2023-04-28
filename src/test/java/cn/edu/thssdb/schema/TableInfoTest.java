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
import java.util.Iterator;

import static org.junit.Assert.assertEquals;

public class TableInfoTest {
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
    s[0] = new Column("col1", IntType.INSTANCE, (byte) 0, (byte) 0, Global.INT_SIZE, 0);
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
  public void testTableIterator() throws Exception {
    table = VarTable.newVarTable(bufferPoolManager);

    Value<?, ?>[] v = new Value[5];
    v[0] = new IntValue(1);
    v[1] = new DoubleValue(2.0);
    v[2] = new StringValue("aaaaaaaaaa", 10);
    v[3] = new FloatValue(3.0f);
    v[4] = new LongValue(4);

    Table tbs = SlotTable.newSlotTable(bufferPoolManager, sh.getTupleSize());

    TableInfo tableInfo = new TableInfo("test", sh, tbs);

    Tuple t = tableInfo.serialize();
    Tuple tt = new Tuple(v, sh);

    RID rid = new RID(0, 0);
    RID rid2 = new RID(1, 0);
    for (int i = 0; i < 1000; i++) {
      table.insert(t, rid);
    }
    System.out.println(rid.toString());

    // iterator
    Iterator<Tuple> iterator = table.iterator();
    // test insert
    int idx = 0;
    while (iterator.hasNext()) {
      Tuple tuple = iterator.next();
      idx++;
      v[0] = new IntValue(idx);
      tt = new Tuple(v, sh);
      TableInfo ti = TableInfo.deserialize(tuple.getValue(), 0, bufferPoolManager);
      assertEquals(ti.getTableName(), "test");
      assertEquals(ti.getSchema().getColumns().length, 5);
      Schema s = ti.getSchema();
      // assert fields of s with sh
      assertEquals(s.getColumns().length, sh.getColumns().length);
      for (int i = 0; i < s.getColumns().length; i++) {
        assertEquals(s.getColumns()[i].getName(), sh.getColumns()[i].getName());
        assertEquals(s.getColumns()[i].getType(), sh.getColumns()[i].getType());
        assertEquals(s.getColumns()[i].isPrimary(), sh.getColumns()[i].isPrimary());
        assertEquals(s.getColumns()[i].Nullable(), sh.getColumns()[i].Nullable());
        assertEquals(s.getColumns()[i].getMaxLength(), sh.getColumns()[i].getMaxLength());
        assertEquals(s.getColumns()[i].getOffset(), sh.getColumns()[i].getOffset());
      }
      Table tbl = ti.getTable();
      tbl.insert(tt, rid);
      if (idx != 1) assertEquals(idx - 1, tbl.getTuple(rid2).getValue(sh, 0).getValue());
      rid2.setPageId(rid.getPageId());
      rid2.setSlotId(rid.getSlotId());
    }
  }

  // after test, delete the test.db file
  @After
  public void tearDown() throws Exception {
    java.nio.file.Files.delete(Paths.get("test.db"));
  }
}
