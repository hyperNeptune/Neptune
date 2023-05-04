package cn.edu.thssdb.execution;

import cn.edu.thssdb.common.TestUtility;
import cn.edu.thssdb.execution.executor.*;
import cn.edu.thssdb.parser.expression.BinaryExpression;
import cn.edu.thssdb.parser.expression.ColumnRefExpression;
import cn.edu.thssdb.parser.expression.ConstantExpression;
import cn.edu.thssdb.parser.expression.Expression;
import cn.edu.thssdb.parser.statement.*;
import cn.edu.thssdb.schema.*;
import cn.edu.thssdb.service.ResultWriter;
import cn.edu.thssdb.storage.Tuple;
import cn.edu.thssdb.type.*;
import cn.edu.thssdb.utils.Pair;
import cn.edu.thssdb.utils.RID;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class executorTest {

  @Before
  public void setUp() throws Exception {
    TestUtility.INSTANCE.setUp();
    TestUtility.INSTANCE.generateMockTable();
    TestUtility.INSTANCE.generateMockTable2();
  }

  @Test
  public void seqScan() {
    // WARN(jyx): you may have to change this (make a txn)
    ExecContext execContext = new ExecContext(null, null, null);
    Executor executor = new SeqScanExecutor(TestUtility.INSTANCE.mockTable, execContext);
    try {
      executor.init();
      Tuple tuple = new Tuple();
      RID rid = new RID();
      int count = 0;
      while (executor.next(tuple, rid)) {
        assertEquals(
            count, tuple.getValue(TestUtility.INSTANCE.mockTable.getSchema(), 0).getValue());
        assertEquals(
            (double) count,
            tuple.getValue(TestUtility.INSTANCE.mockTable.getSchema(), 1).getValue());
        assertEquals(
            "test", tuple.getValue(TestUtility.INSTANCE.mockTable.getSchema(), 2).getValue());
        assertEquals(
            (float) count,
            tuple.getValue(TestUtility.INSTANCE.mockTable.getSchema(), 3).getValue());
        assertEquals(
            (long) count, tuple.getValue(TestUtility.INSTANCE.mockTable.getSchema(), 4).getValue());
        count++;
      }
      assertEquals(1000, count);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Test
  public void projection() {
    // WARN(jyx): you may have to change this (make a txn)
    ExecContext execContext = new ExecContext(null, null, null);
    Executor seqScanExecutor = new SeqScanExecutor(TestUtility.INSTANCE.mockTable, execContext);
    Expression[] expressions = new Expression[1];
    expressions[0] = new ColumnRefExpression("col1");
    Executor projectionExecutor = new projectionExecutor(execContext, seqScanExecutor, expressions);
    try {
      List<Tuple> result = TestUtility.INSTANCE.executionEngine.execute(projectionExecutor);
      List<List<String>> s = ResultWriter.writeTBL(result, projectionExecutor.getOutputSchema());
      for (int i = 0; i < 1000; i++) {
        assertEquals(String.valueOf(i), s.get(i).get(0));
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Test
  public void filter() {
    ExecContext execContext = new ExecContext(null, null, null);
    Executor seqScanExecutor = new SeqScanExecutor(TestUtility.INSTANCE.mockTable, execContext);
    Expression where =
        new BinaryExpression(
            new ColumnRefExpression("col1"), new ConstantExpression(new IntValue(100)), "ge");
    Executor filterExecutor = new filterExecutor(seqScanExecutor, where, execContext);
    Expression[] expressions = new Expression[1];
    expressions[0] = new ColumnRefExpression("col1");
    Executor projectionExecutor = new projectionExecutor(execContext, filterExecutor, expressions);
    try {
      List<Tuple> result = TestUtility.INSTANCE.executionEngine.execute(projectionExecutor);
      List<List<String>> s = ResultWriter.writeTBL(result, projectionExecutor.getOutputSchema());
      System.out.println(s);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Test
  public void insert() {
    ExecContext execContext = new ExecContext(null, null, null);
    Value[] values = new Value[5];
    values[0] = new IntValue(1222);
    values[1] = new DoubleValue(1212);
    values[2] = new StringValue("test", 10);
    values[3] = new FloatValue(1111);
    values[4] = new LongValue(1222);
    Tuple[] ts = new Tuple[1];
    ts[0] = new Tuple(values, TestUtility.INSTANCE.sh);
    Executor insertExecutor = new InsertExecutor(execContext, TestUtility.INSTANCE.mockTable, ts);
    try {
      TestUtility.INSTANCE.executionEngine.execute(insertExecutor);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Test
  public void update() {
    ExecContext execContext = new ExecContext(null, null, null);
    Expression where =
        new BinaryExpression(
            new ColumnRefExpression("col1"), new ConstantExpression(new IntValue(998)), "ge");
    Expression[] expressions = new Expression[1];
    expressions[0] = new ColumnRefExpression("col1");
    Executor seqScanExecutor = new SeqScanExecutor(TestUtility.INSTANCE.mockTable, execContext);
    Executor filterExecutor = new filterExecutor(seqScanExecutor, where, execContext);
    Executor updateExecutor =
        new UpdateExecutor(
            execContext,
            filterExecutor,
            new Pair<>("col1", new ConstantExpression(new IntValue(1020))),
            TestUtility.INSTANCE.mockTable);
    try {
      TestUtility.INSTANCE.executionEngine.execute(updateExecutor);
      filter();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Test
  public void delete() {
    ExecContext execContext = new ExecContext(null, null, null);
    Expression where =
        new BinaryExpression(
            new ColumnRefExpression("col1"), new ConstantExpression(new IntValue(998)), "ge");
    Executor seqScanExecutor = new SeqScanExecutor(TestUtility.INSTANCE.mockTable, execContext);
    Executor filterExecutor = new filterExecutor(seqScanExecutor, where, execContext);
    Executor deleteExecutor =
        new deleteExecutor(execContext, filterExecutor, TestUtility.INSTANCE.mockTable);
    try {
      TestUtility.INSTANCE.executionEngine.execute(deleteExecutor);
      filter();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Test
  public void join() {
    ExecContext execContext = new ExecContext(null, null, null);
    Executor seqScanExecutor1 = new SeqScanExecutor(TestUtility.INSTANCE.mockTable, execContext);
    Executor seqScanExecutor2 = new SeqScanExecutor(TestUtility.INSTANCE.mockTable2, execContext);
    Expression where =
        new BinaryExpression(
            new ColumnRefExpression("col1"), new ColumnRefExpression("col11"), "eq");
    Executor joinExecutor =
        new nestedLoopJoinExecutor(seqScanExecutor1, seqScanExecutor2, where, execContext);
    try {
      List<Tuple> result = TestUtility.INSTANCE.executionEngine.execute(joinExecutor);
      List<List<String>> s = ResultWriter.writeTBL(result, joinExecutor.getOutputSchema());
      System.out.println(s);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @After
  public void tearDown() throws Exception {
    TestUtility.INSTANCE.tearDown();
  }
}
