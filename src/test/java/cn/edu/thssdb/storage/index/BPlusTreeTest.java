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
import java.util.Iterator;

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
    for (int i = 1; i < 19; i++) {
      bpt.insert(new IntValue(i), new RID(i, i), txn);
    }
    System.out.println(bpt.toJson());
  }

  @Test
  public void testBPTIterator() throws Exception {
    BPlusTree bpt = new BPlusTree(bufferPoolManager, IntType.INSTANCE, 3, 4);
    Transaction txn = null;
    for (int i = 1; i < 100; i++) {
      bpt.insert(new IntValue(i), new RID(i, i), txn);
    }
    for (RID rid : bpt) {
      System.out.println(rid);
    }
    System.out.println("another test");
    Iterator<RID> iter = bpt.iterator(new IntValue(51));
    while (iter.hasNext()) {
      System.out.println(iter.next());
    }
  }

  @Test
  public void easyDel() throws Exception {
    BPlusTree bpt = new BPlusTree(bufferPoolManager, IntType.INSTANCE, 3, 4);
    Transaction txn = null;
    for (int i = 1; i < 11; i++) {
      bpt.insert(new IntValue(i), new RID(i, i), txn);
    }
    bpt.remove(new IntValue(5), txn);
    System.out.println(bpt.toJson());
    bpt.remove(new IntValue(2), txn);
    System.out.println(bpt.toJson());
  }

  // after test, delete the test.db file
  @After
  public void tearDown() throws Exception {
    java.nio.file.Files.delete(Paths.get("test.db"));
  }
}
