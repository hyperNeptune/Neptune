package neptune.backend.service;

import neptune.backend.buffer.BufferPoolManager;
import neptune.backend.buffer.LRUReplacer;
import neptune.backend.buffer.ReplaceAlgorithm;
import neptune.backend.concurrency.IsolationLevel;
import neptune.backend.concurrency.LockManager;
import neptune.backend.concurrency.Transaction;
import neptune.backend.concurrency.TransactionManager;
import neptune.backend.execution.ExecContext;
import neptune.backend.execution.ExecutionEngine;
import neptune.backend.execution.Planner;
import neptune.backend.parser.Binder;
import neptune.backend.parser.statement.CreateTbStatement;
import neptune.backend.parser.statement.DropTbStatement;
import neptune.backend.parser.statement.ShowTbStatement;
import neptune.backend.parser.statement.Statement;
import neptune.backend.recovery.LogManager;
import neptune.backend.recovery.LogRecovery;
import neptune.backend.schema.*;
import neptune.backend.storage.DiskManager;
import neptune.backend.storage.Tuple;
import neptune.backend.type.*;
import neptune.common.Global;
import neptune.common.StatusUtil;
import neptune.common.exception.NotBuiltinCommandException;
import neptune.rpc.thrift.ConnectReq;
import neptune.rpc.thrift.ConnectResp;
import neptune.rpc.thrift.DisconnectReq;
import neptune.rpc.thrift.DisconnectResp;
import neptune.rpc.thrift.ExecuteStatementReq;
import neptune.rpc.thrift.ExecuteStatementResp;
import neptune.rpc.thrift.GetTimeReq;
import neptune.rpc.thrift.GetTimeResp;
import neptune.rpc.thrift.IService;
import neptune.rpc.thrift.Status;
import org.apache.thrift.TException;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.*;
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
  private LockManager lockManager_;
  private HashMap<Long, Transaction> transaction_session_;
  private IsolationLevel isolationLevel_ = IsolationLevel.REPEATABLE_READ;
  private boolean activate_recovery_ = false;
  private LogRecovery logRecovery_;

  public HyperNeptuneInstance(String db_file_name) throws Exception {
    // init type system
    StringType st = new StringType();
    IntType it = new IntType();
    DoubleType dt = new DoubleType();
    FloatType ft = new FloatType();
    LongType lt = new LongType();
    BoolType bt = new BoolType();
    // discard them
    diskManager_ = new DiskManager(Paths.get(db_file_name));
    ReplaceAlgorithm replaceAlgorithm = new LRUReplacer(Global.DEFAULT_BUFFER_SIZE);
    bufferPoolManager_ =
        new BufferPoolManager(Global.DEFAULT_BUFFER_SIZE, diskManager_, replaceAlgorithm);
    cdi_ = CimetiereDesInnocents.openCDI(bufferPoolManager_);
    lockManager_ = new LockManager();
    logManager_ = new LogManager(diskManager_);
    transactionManager_ = new TransactionManager(logManager_, lockManager_);
    executionEngine_ = new ExecutionEngine(curDB_, transactionManager_);
    transaction_session_ = new HashMap<>();
    logRecovery_ = new LogRecovery(diskManager_, logManager_);
    if (activate_recovery_) {
      logRecovery_.startRecovery();
    }
  }

  public HyperNeptuneInstance() throws Exception {
    // init type system
    StringType st = new StringType();
    IntType it = new IntType();
    DoubleType dt = new DoubleType();
    FloatType ft = new FloatType();
    LongType lt = new LongType();
    BoolType bt = new BoolType();
    // discard them
    diskManager_ = new DiskManager(Paths.get("tmp.db"));
    ReplaceAlgorithm replaceAlgorithm = new LRUReplacer(Global.DEFAULT_BUFFER_SIZE);
    bufferPoolManager_ =
        new BufferPoolManager(Global.DEFAULT_BUFFER_SIZE, diskManager_, replaceAlgorithm);
    cdi_ = CimetiereDesInnocents.createCDI(bufferPoolManager_);
    lockManager_ = new LockManager();
    logManager_ = new LogManager(diskManager_);
    transactionManager_ = new TransactionManager(logManager_, lockManager_);
    executionEngine_ = new ExecutionEngine(curDB_, transactionManager_);
    transaction_session_ = new HashMap<>();
    logRecovery_ = new LogRecovery(diskManager_, logManager_);
    if (activate_recovery_) {
      logRecovery_.startRecovery();
    }
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
      if (req.statement.equals("begin transaction")) {
        Transaction txn = transactionManager_.create_and_begin(isolationLevel_);
        transaction_session_.put(req.getSessionId(), txn);
        return new ExecuteStatementResp(StatusUtil.success(), false);
      }
      if (req.statement.equals("commit;")) {
        Transaction txn = transaction_session_.get(req.getSessionId());
        transactionManager_.commit(txn);
        return new ExecuteStatementResp(StatusUtil.success(), false);
      }

      if (transaction_session_.get(req.getSessionId()) == null) {
        Transaction txn = transactionManager_.create_and_begin(isolationLevel_);
        ExecuteStatementResp resp = executeStatementTxn(req, txn);
        transactionManager_.commit(txn);
        return resp;
      } else {
        ExecuteStatementResp resp =
            executeStatementTxn(req, transaction_session_.get(req.getSessionId()));
        return resp;
      }

    } catch (Exception e) {
      e.printStackTrace();
      return new ExecuteStatementResp(StatusUtil.success(e.getMessage()), false);
    }
  }

  // we need this because we cannot afford to shut down the whole system when sql statement errs
  public ExecuteStatementResp executeStatementTxn(ExecuteStatementReq req, Transaction txn)
      throws Exception {
    if (req.getSessionId() < 0) {
      throw new Exception("You are not connected. Please connect first.");
    }
    // handle builtin commands
    try {
      return executeBuiltinStatement(req);
    } catch (NotBuiltinCommandException e) {
      // not builtin command, continue
    }

    // we finally came to the real SQL statements
    if (curDB_ == null) {
      throw new Exception("No database selected.");
    }

    Binder binder = new Binder(curDB_);
    binder.parseAndBind(req.statement);
    for (Statement stmt : binder) {
      // some statements can be executed directly
      // they are:
      // - create table
      // - drop table
      // - show meta
      // they are handled by this switch-case clause
      switch (stmt.getType()) {
        case CREATE_TABLE:
          CreateTbStatement createTableStatement = (CreateTbStatement) stmt;
          curDB_.createTable(
              createTableStatement.getTableName(),
              new Schema(createTableStatement.getColumns(), createTableStatement.getTableName()));
          return new ExecuteStatementResp(StatusUtil.success(), false);
        case DROP_TABLE:
          DropTbStatement dropTableStatement = (DropTbStatement) stmt;
          curDB_.dropTable(dropTableStatement.tableName);
          return new ExecuteStatementResp(StatusUtil.success(), false);
        case SHOW_TABLE:
          ShowTbStatement showMetaStatement = (ShowTbStatement) stmt;
          return showTable(showMetaStatement.getTableInfo());
        default:
          break;
      }

      // other statements need to be executed by the execution engine
      // insert
      // select
      // update
      // delete
      ExecContext execContext = makeExecContext(txn);
      Planner planner = new Planner(curDB_, execContext);
      planner.plan(stmt);
      List<Tuple> result = executionEngine_.execute(planner.getPlan());
      Schema sh = planner.getPlan().getOutputSchema();

      // output results
      List<String> lshdr = ResultWriter.writeHDR(sh);
      List<List<String>> lstuple = ResultWriter.writeTBL(result, sh);
      ExecuteStatementResp resp = new ExecuteStatementResp(StatusUtil.success(), true);
      resp.setColumnsList(lshdr);
      resp.setRowList(lstuple);
      return resp;
    }
    return new ExecuteStatementResp(StatusUtil.success(), false);
  }

  private ExecContext makeExecContext(Transaction txn) {
    ExecContext execContext =
        new ExecContext(txn, curDB_, bufferPoolManager_, lockManager_, transactionManager_);
    return execContext;
  }

  private ExecuteStatementResp executeBuiltinStatement(ExecuteStatementReq req) throws Exception {
    // trailing semicolon is removed
    String cmd = req.statement.substring(0, req.statement.length() - 1);
    if (cmd.equals("show databases")) {
      return showDatabases();
    }
    if (cmd.startsWith("create database")) {
      return createDatabase(req.statement);
    }
    if (cmd.startsWith("drop database")) {
      return dropDatabase(req.statement);
    }
    if (cmd.startsWith("use")) {
      return useDatabase(req.statement);
    }
    if (cmd.startsWith("show tables")) {
      return showTables(req.statement);
    }
    if (cmd.startsWith("help")) {
      return help();
    }
    throw new NotBuiltinCommandException();
  }

  // built-in
  private ExecuteStatementResp showTable(TableInfo tableInfo) {
    ExecuteStatementResp resp = new ExecuteStatementResp();
    resp.setHasResult(true);
    resp.setStatus(StatusUtil.success());
    resp.setColumnsList(Arrays.asList("column_name", "type", "primary_key"));
    List<List<String>> rows = new ArrayList<>();
    for (Column columnInfo : tableInfo.getSchema().getColumns()) {
      rows.add(
          Arrays.asList(
              columnInfo.getName(),
              columnInfo.getType().toString(),
              columnInfo.isPrimary() == 1 ? "true" : "false"));
    }
    resp.setRowList(rows);
    return resp;
  }

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
    if (tokens.length != 2) {
      return new ExecuteStatementResp(StatusUtil.fail("Invalid statement."), false);
    }
    String dbName = tokens[1];
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

  public void close() throws IOException {
    bufferPoolManager_.flushAllPages();
  }
}
