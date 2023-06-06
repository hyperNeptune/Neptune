package cn.edu.thssdb.storage.index;

import cn.edu.thssdb.buffer.BufferPoolManager;
import cn.edu.thssdb.buffer.LRUReplacer;
import cn.edu.thssdb.buffer.ReplaceAlgorithm;
import cn.edu.thssdb.concurrency.Transaction;
import cn.edu.thssdb.storage.DiskManager;
import cn.edu.thssdb.type.*;
import cn.edu.thssdb.utils.Global;
import cn.edu.thssdb.utils.RID;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.file.Paths;


public class BPlusTreeTest {
  private BufferPoolManager bufferPoolManager;

  @Before
  public void setUp() throws Exception {
    ReplaceAlgorithm replaceAlgorithm = new LRUReplacer(Global.DEFAULT_BUFFER_SIZE);
    DiskManager diskManager = new DiskManager(Paths.get("test.db"));
    bufferPoolManager =
      new BufferPoolManager(Global.DEFAULT_BUFFER_SIZE, diskManager, replaceAlgorithm);
  }

  @Test
  public void testBPTreeIns() throws Exception {
    BPlusTree bpt = new BPlusTree(bufferPoolManager, IntType.INSTANCE, 3);
    Transaction txn = null;
    bpt.insert(new IntValue(1), new RID(1, 1), txn);
    bpt.insert(new IntValue(2), new RID(2, 2), txn);
    bpt.insert(new IntValue(3), new RID(3, 3), txn);
    bpt.print();
  }

  // after test, delete the test.db file
  @After
  public void tearDown() throws Exception {
    java.nio.file.Files.delete(Paths.get("test.db"));
  }
}
