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
    // split at maxSize
    BPlusTree bpt = new BPlusTree(bufferPoolManager, IntType.INSTANCE, 3, 4);
    Transaction txn = null;
    bpt.insert(new IntValue(1), new RID(1, 1), txn);
    bpt.insert(new IntValue(2), new RID(2, 2), txn);
    bpt.insert(new IntValue(3), new RID(3, 3), txn);
    bpt.insert(new IntValue(4), new RID(4, 4), txn);
    bpt.insert(new IntValue(5), new RID(5, 5), txn);
    bpt.insert(new IntValue(6), new RID(6, 6), txn);
    bpt.insert(new IntValue(7), new RID(7, 7), txn);
    bpt.insert(new IntValue(8), new RID(8, 8), txn);
    bpt.insert(new IntValue(9), new RID(9, 9), txn);
    bpt.insert(new IntValue(10), new RID(10, 10), txn);
    System.out.println(bpt.toJson());
  }

  // after test, delete the test.db file
  @After
  public void tearDown() throws Exception {
    java.nio.file.Files.delete(Paths.get("test.db"));
  }
}
