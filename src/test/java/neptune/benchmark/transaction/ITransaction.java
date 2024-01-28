package neptune.benchmark.transaction;

import neptune.benchmark.common.Client;
import org.apache.thrift.TException;

public interface ITransaction {

  void execute(Client client) throws TException;

  int getTransactionSize();
}
