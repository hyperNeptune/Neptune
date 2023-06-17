package cn.edu.thssdb.execution;

import cn.edu.thssdb.common.TestUtility;
import cn.edu.thssdb.concurrency.IsolationLevel;
import cn.edu.thssdb.concurrency.Transaction;
import cn.edu.thssdb.execution.executor.Executor;
import cn.edu.thssdb.parser.Binder;
import cn.edu.thssdb.parser.statement.Statement;
import cn.edu.thssdb.service.ResultWriter;
import cn.edu.thssdb.storage.Tuple;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

public class selectTest {

  @Before
  public void setUp() throws Exception {
    TestUtility.INSTANCE.setUp();
    TestUtility.INSTANCE.generateMockTable();
  }

  @Test
  public void select() throws Exception {
    Binder binder = new Binder(TestUtility.INSTANCE.curDB);
    binder.parseAndBind("select * from test where col5 > 100");
    for (Statement s : binder) {
      ExecContext ctx =
          new ExecContext(
              new Transaction(1, IsolationLevel.SERIALIZED),
              null,
              null,
              TestUtility.INSTANCE.lockManager,
              null);
      Planner planner = new Planner(TestUtility.INSTANCE.curDB, ctx);
      planner.plan(s);
      Executor executor = planner.getPlan();
      List<Tuple> result = TestUtility.INSTANCE.executionEngine.execute(executor);
      List<List<String>> lls = ResultWriter.writeTBL(result, executor.getOutputSchema());
      System.out.println(lls);
    }
  }

  @After
  public void tearDown() throws Exception {
    TestUtility.INSTANCE.tearDown();
  }
}
