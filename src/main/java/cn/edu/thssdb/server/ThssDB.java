package cn.edu.thssdb.server;

import cn.edu.thssdb.rpc.thrift.IService;
import cn.edu.thssdb.service.HyperNeptuneInstance;
import cn.edu.thssdb.utils.Global;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class ThssDB {

  private static final Logger logger = LoggerFactory.getLogger(ThssDB.class);

  private static HyperNeptuneInstance handler;
  private static IService.Processor<?> processor;
  private static TServerSocket transport;
  private static TServer server;

  public static ThssDB INSTANCE = new ThssDB();

  public static void main(String[] args) throws Exception {
    ThssDB server = ThssDB.INSTANCE;
    server.start();
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
    handler = new HyperNeptuneInstance("tmp.db");
    processor = new IService.Processor(handler);
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
