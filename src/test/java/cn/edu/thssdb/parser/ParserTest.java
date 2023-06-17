package cn.edu.thssdb.parser;

import cn.edu.thssdb.buffer.BufferPoolManager;
import cn.edu.thssdb.buffer.LRUReplacer;
import cn.edu.thssdb.buffer.ReplaceAlgorithm;
import cn.edu.thssdb.parser.statement.*;
import cn.edu.thssdb.schema.*;
import cn.edu.thssdb.storage.DiskManager;
import cn.edu.thssdb.type.*;
import cn.edu.thssdb.utils.Global;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.file.Paths;
import java.util.Iterator;

import static cn.edu.thssdb.parser.statement.StatementType.CREATE_TABLE;
import static org.junit.Assert.assertEquals;

public class ParserTest {
  private DiskManager diskManager;
  private BufferPoolManager bufferPoolManager;
  private ReplaceAlgorithm replaceAlgorithm;
  private CimetiereDesInnocents cdi;
  private Catalog curDB_;
  private Table table;
  private Schema sh;

  @Before
  public void setUp() throws Exception {
    replaceAlgorithm = new LRUReplacer(100);
    diskManager = new DiskManager(Paths.get("test.db"));
    bufferPoolManager = new BufferPoolManager(100, diskManager, replaceAlgorithm);
    cdi = CimetiereDesInnocents.createCDI(bufferPoolManager);
    curDB_ = cdi.createDatabase("test");
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
  public void testParser() throws Exception {
    // new table testp
    curDB_.createTable("testp", sh);
    Binder binder = new Binder(curDB_);
    binder.parseAndBind(
        "create table test (col1 int primary key, col2 double, col3 string(10), col4 float, col5 long);"
            + "insert into testp values (1, 2.0, 'hello', 3.0, 4);"
            + "insert into testp values (2, 3.0, 'world', 4.0, 5);"
            + "update testp set col2 = 4.0 where col1 = 1;"
            + "select * from testp where col1 = 1;");
    // iterate binder, and assert statement
    Iterator<Statement> iterator = binder.iterator();
    // create table
    if (iterator.hasNext()) {
      Statement statement = iterator.next();
      assertEquals(statement.getType(), CREATE_TABLE);
      CreateTbStatement createTbStatement = (CreateTbStatement) statement;
      assertEquals(createTbStatement.getTableName(), "test");
      assertEquals(createTbStatement.getColumns().length, 5);
      assertEquals(createTbStatement.getColumns()[0].getName(), "col1");
      assertEquals(createTbStatement.getColumns()[0].getType(), IntType.INSTANCE);
      assertEquals(createTbStatement.getColumns()[1].getName(), "col2");
      assertEquals(createTbStatement.getColumns()[1].getType(), DoubleType.INSTANCE);
      assertEquals(createTbStatement.getColumns()[2].getName(), "col3");
      assertEquals(createTbStatement.getColumns()[2].getType(), StringType.getVarCharType(10));
      assertEquals(createTbStatement.getColumns()[3].getName(), "col4");
      assertEquals(createTbStatement.getColumns()[3].getType(), FloatType.INSTANCE);
      assertEquals(createTbStatement.getColumns()[4].getName(), "col5");
      assertEquals(createTbStatement.getColumns()[4].getType(), LongType.INSTANCE);
    }

    // insert into
    if (iterator.hasNext()) {
      Statement statement = iterator.next();
      assertEquals(statement.getType(), StatementType.INSERT);
      InsertStatement insertStatement = (InsertStatement) statement;
      Schema schema = insertStatement.getTable().getSchema();
      assertEquals(insertStatement.getTable().getTableName(), "testp");
      assertEquals(insertStatement.getTuple()[0].getValue(sh, 0).getValue(), 1);
      assertEquals(insertStatement.getTuple()[0].getValue(sh, 1).getValue(), 2.0);
      assertEquals(insertStatement.getTuple()[0].getValue(sh, 2).getValue(), "hello");
      assertEquals(insertStatement.getTuple()[0].getValue(sh, 3).getValue(), 3.0f);
      assertEquals(insertStatement.getTuple()[0].getValue(sh, 4).getValue(), 4L);
    }

    // insert into
    if (iterator.hasNext()) {
      Statement statement = iterator.next();
      assertEquals(statement.getType(), StatementType.INSERT);
      InsertStatement insertStatement = (InsertStatement) statement;
      Schema schema = insertStatement.getTable().getSchema();
      assertEquals(insertStatement.getTable().getTableName(), "testp");
      assertEquals(insertStatement.getTuple()[0].getValue(sh, 0).getValue(), 2);
      assertEquals(insertStatement.getTuple()[0].getValue(sh, 1).getValue(), 3.0);
      assertEquals(insertStatement.getTuple()[0].getValue(sh, 2).getValue(), "world");
      assertEquals(insertStatement.getTuple()[0].getValue(sh, 3).getValue(), 4.0f);
      assertEquals(insertStatement.getTuple()[0].getValue(sh, 4).getValue(), 5L);
    }

    // update
    if (iterator.hasNext()) {
      Statement statement = iterator.next();
      assertEquals(statement.getType(), StatementType.UPDATE);
      UpdateStatement updateStatement = (UpdateStatement) statement;
      assertEquals(updateStatement.getTable().getTableName(), "testp");
      assertEquals(updateStatement.getUpdateValue().left, "col2");
      assertEquals(updateStatement.getUpdateValue().right.evaluation(null, null).getValue(), 4.0);
    }
  }

  // after test, delete the test.db file
  @After
  public void tearDown() throws Exception {
    // remove the test.db file
    java.nio.file.Files.deleteIfExists(Paths.get("test.db"));
  }
}
