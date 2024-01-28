package neptune.execution;

import neptune.backend.concurrency.IsolationLevel;
import neptune.backend.concurrency.Transaction;
import neptune.backend.execution.ExecContext;
import neptune.backend.execution.Planner;
import neptune.backend.execution.executor.Executor;
import neptune.backend.parser.Binder;
import neptune.backend.parser.statement.Statement;
import neptune.backend.service.ResultWriter;
import neptune.backend.storage.Tuple;
import neptune.common.TestUtility;
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
