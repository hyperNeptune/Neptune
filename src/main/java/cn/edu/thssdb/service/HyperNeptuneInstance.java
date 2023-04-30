package cn.edu.thssdb.service;

import cn.edu.thssdb.buffer.BufferPoolManager;
import cn.edu.thssdb.buffer.LRUReplacer;
import cn.edu.thssdb.buffer.ReplaceAlgorithm;
import cn.edu.thssdb.concurrency.Transaction;
import cn.edu.thssdb.concurrency.TransactionManager;
import cn.edu.thssdb.execution.ExecutionEngine;
import cn.edu.thssdb.execution.plan.LogicalGenerator;
import cn.edu.thssdb.execution.plan.LogicalPlan;
import cn.edu.thssdb.parser.Binder;
import cn.edu.thssdb.parser.statement.Statement;
import cn.edu.thssdb.recovery.LogManager;
import cn.edu.thssdb.rpc.thrift.ConnectReq;
import cn.edu.thssdb.rpc.thrift.ConnectResp;
import cn.edu.thssdb.rpc.thrift.DisconnectReq;
import cn.edu.thssdb.rpc.thrift.DisconnectResp;
import cn.edu.thssdb.rpc.thrift.ExecuteStatementReq;
import cn.edu.thssdb.rpc.thrift.ExecuteStatementResp;
import cn.edu.thssdb.rpc.thrift.GetTimeReq;
import cn.edu.thssdb.rpc.thrift.GetTimeResp;
import cn.edu.thssdb.rpc.thrift.IService;
import cn.edu.thssdb.rpc.thrift.Status;
import cn.edu.thssdb.schema.Catalog;
import cn.edu.thssdb.schema.CimetiereDesInnocents;
import cn.edu.thssdb.storage.DiskManager;
import cn.edu.thssdb.utils.Global;
import cn.edu.thssdb.utils.StatusUtil;
import org.apache.thrift.TException;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class HyperNeptuneInstance implements IService.Iface {

  private static final AtomicInteger sessionCnt = new AtomicInteger(0);
  private DiskManager diskManager_;
  private BufferPoolManager bufferPoolManager_;
  private CimetiereDesInnocents cdi_;
  private Catalog curDB_;
  private ExecutionEngine executionEngine_;
  private TransactionManager transactionManager_;
  private LogManager logManager_;
  private static final String builtinCommandIndicator = "㵘";

  public HyperNeptuneInstance(String db_file_name) throws Exception {
    diskManager_ = new DiskManager(Paths.get(db_file_name));
    ReplaceAlgorithm replaceAlgorithm = new LRUReplacer(Global.DEFAULT_BUFFER_SIZE);
    bufferPoolManager_ =
        new BufferPoolManager(Global.DEFAULT_BUFFER_SIZE, diskManager_, replaceAlgorithm);
    cdi_ = CimetiereDesInnocents.createCDI(bufferPoolManager_);
    LogManager logManager_ = new LogManager(diskManager_);
    transactionManager_ = new TransactionManager(logManager_);
    executionEngine_ = new ExecutionEngine(curDB_, transactionManager_);
  }

  @Override
  public GetTimeResp getTime(GetTimeReq req) throws TException {
    GetTimeResp resp = new GetTimeResp();
    resp.setTime(new Date().toString());
    resp.setStatus(new Status(Global.SUCCESS_CODE));
    return resp;
  }

  @Override
  public ConnectResp connect(ConnectReq req) throws TException {
    return new ConnectResp(StatusUtil.success(), sessionCnt.getAndIncrement());
  }

  @Override
  public DisconnectResp disconnect(DisconnectReq req) throws TException {
    return new DisconnectResp(StatusUtil.success());
  }

  @Override
  public ExecuteStatementResp executeStatement(ExecuteStatementReq req) {
    if (req.getSessionId() < 0) {
      return new ExecuteStatementResp(
          StatusUtil.fail("You are not connected. Please connect first."), false);
    }
    try {
      Transaction txn = transactionManager_.begin();
      ExecuteStatementResp resp = executeStatementTxn(req);
      transactionManager_.commit(txn);
      return resp;
    } catch (Exception e) {
      e.printStackTrace();
      return new ExecuteStatementResp(StatusUtil.fail(e.getMessage()), false);
    }
  }

  public ExecuteStatementResp executeStatementTxn(ExecuteStatementReq req) throws Exception {
    if (req.getSessionId() < 0) {
      throw new Exception("You are not connected. Please connect first.");
    }
    // handle builtin commands
    if (req.statement.startsWith(builtinCommandIndicator)) {
      if (req.statement.equals("㵘show databases")) {
        return showDatabases();
      }
      if (req.statement.startsWith("㵘create database")) {
        return createDatabase(req.statement);
      }
      if (req.statement.startsWith("㵘drop database")) {
        return dropDatabase(req.statement);
      }
      if (req.statement.startsWith("㵘use database")) {
        return useDatabase(req.statement);
      }
      if (req.statement.startsWith("㵘show tables")) {
        return showTables(req.statement);
      }
      if (req.statement.startsWith("㵘help")) {
        return help();
      }
      throw new Exception("Invalid builtin command.");
    }

    // we finally came to the real SQL statements
    if (curDB_ == null) {
      throw new Exception("No database selected.");
    }

    Binder binder = new Binder(curDB_);
    binder.parseAndBind(req.statement);
    for (Statement stmt : binder) {
      // do something
      LogicalPlan plan = LogicalGenerator.generate(stmt);
      switch (plan.getType()) {
        case CREATE_DB:
          System.out.println("[DEBUG] " + plan);
          return new ExecuteStatementResp(StatusUtil.success(), false);
        default:
      }
    }

    return null;
  }

  // built-in
  private ExecuteStatementResp showTables(String statement) {
    String[] tokens = statement.split("\\s+");
    if (tokens.length != 3) {
      return new ExecuteStatementResp(StatusUtil.fail("Invalid statement."), false);
    }
    Catalog catalog = cdi_.useDatabase(tokens[2]);
    if (catalog == null) {
      return new ExecuteStatementResp(StatusUtil.fail("Database does not exist."), false);
    }
    ExecuteStatementResp resp = new ExecuteStatementResp();
    resp.setHasResult(true);
    resp.setStatus(StatusUtil.success());
    resp.setColumnsList(Arrays.asList("table_name"));
    List<List<String>> rows = new ArrayList<>();
    String[] tableNames = catalog.list();
    for (String tableName : tableNames) {
      rows.add(Arrays.asList(tableName));
    }
    resp.setRowList(rows);
    return resp;
  }

  private ExecuteStatementResp showDatabases() {
    if (cdi_.listDatabases().length == 0) {
      throw new RuntimeException("No database exists.");
    }
    ExecuteStatementResp resp = new ExecuteStatementResp();
    resp.setHasResult(true);
    resp.setStatus(StatusUtil.success());
    resp.setColumnsList(Arrays.asList("database_name"));
    List<List<String>> rows = new ArrayList<>();
    String[] dbNames = cdi_.listDatabases();
    for (String dbName : dbNames) {
      rows.add(Arrays.asList(dbName));
    }
    resp.setRowList(rows);
    return resp;
  }

  private ExecuteStatementResp createDatabase(String statement) {
    String[] tokens = statement.split("\\s+");
    if (tokens.length != 3) {
      return new ExecuteStatementResp(StatusUtil.fail("Invalid statement."), false);
    }
    String dbName = tokens[2];
    if (cdi_.hasDatabase(dbName)) {
      return new ExecuteStatementResp(StatusUtil.fail("Database already exists."), false);
    }
    try {
      cdi_.createDatabase(dbName);
    } catch (Exception e) {
      return new ExecuteStatementResp(StatusUtil.fail("Failed to create database."), false);
    }
    return new ExecuteStatementResp(StatusUtil.success(), false);
  }

  private ExecuteStatementResp dropDatabase(String statement) {
    String[] tokens = statement.split("\\s+");
    if (tokens.length != 3) {
      return new ExecuteStatementResp(StatusUtil.fail("Invalid statement."), false);
    }
    String dbName = tokens[2];
    if (!cdi_.hasDatabase(dbName)) {
      return new ExecuteStatementResp(StatusUtil.fail("Database does not exist."), false);
    }
    try {
      cdi_.dropDatabase(dbName);
    } catch (Exception e) {
      return new ExecuteStatementResp(StatusUtil.fail("Failed to drop database."), false);
    }
    return new ExecuteStatementResp(StatusUtil.success(), false);
  }

  private ExecuteStatementResp useDatabase(String statement) {
    String[] tokens = statement.split("\\s+");
    if (tokens.length != 3) {
      return new ExecuteStatementResp(StatusUtil.fail("Invalid statement."), false);
    }
    String dbName = tokens[2];
    if (!cdi_.hasDatabase(dbName)) {
      return new ExecuteStatementResp(StatusUtil.fail("Database does not exist."), false);
    }
    try {
      curDB_ = cdi_.useDatabase(dbName);
      executionEngine_.setCatalog(curDB_);
    } catch (Exception e) {
      return new ExecuteStatementResp(StatusUtil.fail("Failed to use database."), false);
    }
    return new ExecuteStatementResp(StatusUtil.success(), false);
  }

  private ExecuteStatementResp help() {
    ExecuteStatementResp resp = new ExecuteStatementResp();
    resp.setHasResult(true);
    resp.setStatus(StatusUtil.success());
    resp.setColumnsList(Arrays.asList("command", "description"));
    resp.setRowList(
        Arrays.asList(
            Arrays.asList("㵘show databases", "show all databases"),
            Arrays.asList("㵘create database <db_name>", "create a database"),
            Arrays.asList("㵘drop database <db_name>", "drop a database"),
            Arrays.asList("㵘use database <db_name>", "use a database"),
            Arrays.asList("㵘help", "show this help message")));
    return resp;
  }
}
