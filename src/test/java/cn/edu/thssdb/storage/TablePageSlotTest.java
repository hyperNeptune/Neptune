package cn.edu.thssdb.storage;

import cn.edu.thssdb.schema.*;
import cn.edu.thssdb.type.*;
import cn.edu.thssdb.utils.Global;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.file.Paths;

import static org.junit.Assert.assertEquals;

public class TablePageSlotTest {
  private DiskManager diskManager;
  private Page page;

  @Before
  public void setUp() throws Exception {
    diskManager = new DiskManager(Paths.get("test.db"));
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

    Value<?, ?>[] v = new Value[5];
    v[0] = new IntValue(1);
    v[1] = new DoubleValue(2.0);
    v[2] = new StringValue("aaaaaaaaaa", 10);
    v[3] = new FloatValue(3.0f);
    v[4] = new LongValue(4);

    Tuple t = new Tuple(v, sh);
    System.out.println(sh);
    t.print(sh);

    t.serialize(page.getData(), 20);
    Tuple tt = Tuple.deserialize(page.getData(), 20, sh);
    tt.print(sh);
    assertEquals(t.toString(sh), tt.toString(sh));

    // test TablePageSlot
    TablePageSlot tablePage = new TablePageSlot(1, sh.getSize());

    for (int i = 0; i < 131; i++) {
      tablePage.insertTuple(t);
    }

    Tuple tt2 = tablePage.getTuple(0, sh);
    tt2.print(sh);
    assertEquals(t.toString(sh), tt2.toString(sh));
    tablePage.print(sh);

    tablePage.deleteTuple(0);
    tablePage.print(sh);

    tablePage.insertTuple(t);
    tablePage.print(sh);
  }

  // after test, delete the test.db file
  @After
  public void tearDown() throws Exception {
    java.nio.file.Files.delete(Paths.get("test.db"));
  }
}
