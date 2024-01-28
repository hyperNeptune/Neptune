package neptune.server;

import neptune.rpc.thrift.IService;
import neptune.service.HyperNeptuneInstance;
import neptune.utils.Global;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class NeptuneDB {

  private static final Logger logger = LoggerFactory.getLogger(NeptuneDB.class);

  private static HyperNeptuneInstance handler;
  private static IService.Processor<?> processor;
  private static TServerSocket transport;
  private static TServer server;

  public static NeptuneDB INSTANCE = new NeptuneDB();

  public static void main(String[] args) throws Exception {
    NeptuneDB server = NeptuneDB.INSTANCE;
    if (args.length != 0) {
      server.start(args[0]);
    } else {
      server.start();
    }
    // call handler.close() when exit
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  logger.info("Gracefully killing this database!\n");
                  try {
                    handler.close();
                  } catch (IOException e) {
                    throw new RuntimeException(e);
                  }
                }));
  }

  private void start() throws Exception {
    handler = new HyperNeptuneInstance();
    processor = new IService.Processor<>(handler);
    Runnable setup = () -> setUp(processor);
    new Thread(setup).start();
  }

  private void start(String fileName) throws Exception {
    handler = new HyperNeptuneInstance(fileName);
    processor = new IService.Processor<>(handler);
    Runnable setup = () -> setUp(processor);
    new Thread(setup).start();
  }

  private static void setUp(IService.Processor<?> processor) {
    try {
      transport = new TServerSocket(Global.DEFAULT_SERVER_PORT);
      server = new TThreadPoolServer(new TThreadPoolServer.Args(transport).processor(processor));
      logger.info("Starting ThssDB ...");
      server.serve();
    } catch (TTransportException e) {
      logger.error(e.getMessage());
    }
  }
}
