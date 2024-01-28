package neptune.benchmark.transaction;

import neptune.benchmark.common.Client;
import org.apache.thrift.TException;

public class SingleSQLTransaction implements ITransaction {

  String sql;

  public SingleSQLTransaction(String sql) {
    this.sql = sql;
  }

  @Override
  public void execute(Client client) throws TException {
    client.executeStatement(sql);
  }

  @Override
  public int getTransactionSize() {
    return 1;
  }
}
