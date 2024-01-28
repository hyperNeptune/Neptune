package neptune.benchmark.executor;

import neptune.benchmark.common.Client;
import neptune.benchmark.common.Constants;
import neptune.benchmark.common.TableReadWriteUtil;
import neptune.benchmark.common.TableSchema;
import neptune.benchmark.generator.BaseDataGenerator;
import neptune.benchmark.generator.ConcurrentDataGenerator;
import neptune.rpc.thrift.ExecuteStatementResp;
import org.apache.thrift.TException;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class ConcurrentTestExecutor extends TestExecutor {

  private static final Logger LOGGER = LoggerFactory.getLogger(ConcurrentTestExecutor.class);

  private BaseDataGenerator dataGenerator;
  private Map<String, TableSchema> schemaMap;
  private Client client1;
  private Client client2;
  private static final int rowNum = 1000;

  public ConcurrentTestExecutor() throws TException {
    dataGenerator = new ConcurrentDataGenerator();
    schemaMap = dataGenerator.getSchemaMap();
    client1 = new Client();
    client2 = new Client();
  }

  public void createAndUseDB() throws TException {
    client1.executeStatement("drop database db_concurrent;");
    // make sure database not exist, it's ok to ignore the error
    ExecuteStatementResp resp = client1.executeStatement("create database db_concurrent;");
    Assert.assertEquals(Constants.SUCCESS_STATUS_CODE, resp.status.code);
    LOGGER.info("Create database db_concurrent finished");
    ExecuteStatementResp resp1 = client1.executeStatement("use db_concurrent;");
    Assert.assertEquals(Constants.SUCCESS_STATUS_CODE, resp1.status.code);
    ExecuteStatementResp resp2 = client2.executeStatement("use db_concurrent;");
    Assert.assertEquals(Constants.SUCCESS_STATUS_CODE, resp2.status.code);
    LOGGER.info("Use db_concurrent finished");
  }

  public void concurrentCreateTable() {
    CompletableFuture.allOf(
            CompletableFuture.runAsync(
                () -> {
                  try {
                    LOGGER.info("Create concurrent_table_1 started");
                    createTable(schemaMap.get("concurrent_table_1"), client1);
                    LOGGER.info("Create concurrent_table_1 finished");
                  } catch (TException e) {
                    e.printStackTrace();
                  }
                }),
            CompletableFuture.runAsync(
                () -> {
                  try {
                    LOGGER.info("Create concurrent_table_2 started");
                    createTable(schemaMap.get("concurrent_table_2"), client2);
                    LOGGER.info("Create concurrent_table_2 finished");
                  } catch (TException e) {
                    e.printStackTrace();
                  }
                }))
        .join();
  }

  public void concurrentInsertAndQuery() {
    CompletableFuture.allOf(
            CompletableFuture.runAsync(
                () -> {
                  try {
                    LOGGER.info("Insert and query concurrent_table_1 started");
                    TableReadWriteUtil.insertData(
                        schemaMap.get("concurrent_table_1"), client1, dataGenerator, rowNum);
                    TableReadWriteUtil.queryAndCheckData(
                        schemaMap.get("concurrent_table_1"), client1, dataGenerator, rowNum);
                    LOGGER.info("Insert and query concurrent_table_1 finished");
                  } catch (TException e) {
                    e.printStackTrace();
                  }
                }),
            CompletableFuture.runAsync(
                () -> {
                  try {
                    LOGGER.info("Insert and query concurrent_table_2 started");
                    TableReadWriteUtil.insertData(
                        schemaMap.get("concurrent_table_2"), client2, dataGenerator, rowNum);
                    TableReadWriteUtil.queryAndCheckData(
                        schemaMap.get("concurrent_table_2"), client2, dataGenerator, rowNum);
                    LOGGER.info("Insert and query concurrent_table_2 finished");
                  } catch (TException e) {
                    e.printStackTrace();
                  }
                }))
        .join();
  }

  @Override
  public void close() {
    try {
      client1.executeStatement("drop database db_concurrent");
    } catch (TException e) {
      LOGGER.error("{}", e.getMessage(), e);
    }
    client1.close();
    client2.close();
  }
}
