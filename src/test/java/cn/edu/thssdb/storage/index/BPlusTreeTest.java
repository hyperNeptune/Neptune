package cn.edu.thssdb.storage.index;

import cn.edu.thssdb.buffer.BufferPoolManager;
import cn.edu.thssdb.buffer.LRUReplacer;
import cn.edu.thssdb.buffer.ReplaceAlgorithm;
import cn.edu.thssdb.concurrency.IsolationLevel;
import cn.edu.thssdb.concurrency.Transaction;
import cn.edu.thssdb.storage.DiskManager;
import cn.edu.thssdb.type.*;
import cn.edu.thssdb.utils.Global;
import cn.edu.thssdb.utils.RID;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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
    Transaction txn = new Transaction(111, IsolationLevel.READ_COMMITTED);
    for (int i = 1; i < 19; i++) {
      bpt.insert(new IntValue(i), new RID(i, i), txn);
    }
    // System.out.println(bpt.toJson());
  }

  @Test
  public void testBPTIterator() throws Exception {
    BPlusTree bpt = new BPlusTree(bufferPoolManager, IntType.INSTANCE, 3, 4);
    Transaction txn = new Transaction(111, IsolationLevel.READ_COMMITTED);
    for (int i = 1; i < 100; i++) {
      bpt.insert(new IntValue(i), new RID(i, i), txn);
    }
//    for (RID rid : bpt) {
//      System.out.println(rid);
//    }
    System.out.println("another test");
    Iterator<RID> iter = bpt.iterator(new IntValue(51));
//    while (iter.hasNext()) {
//      System.out.println(iter.next());
//    }
  }

  @Test
  public void easyDel() throws Exception {
    BPlusTree bpt = new BPlusTree(bufferPoolManager, IntType.INSTANCE, 3, 4);
    Transaction txn = new Transaction(111, IsolationLevel.READ_COMMITTED);
    for (int i = 1; i < 11; i++) {
      bpt.insert(new IntValue(i), new RID(i, i), txn);
    }
    bpt.remove(new IntValue(5), txn);
    bpt.remove(new IntValue(2), txn);
    // System.out.println(bpt.toJson());
  }

  @Test
  public void insertionTestRoot() throws Exception {
    Value<?, ?> key = new IntValue(42);
    RID rid = new RID(42, 12122);
    BPlusTree bpt = new BPlusTree(bufferPoolManager, IntType.INSTANCE, 3, 4);
    Transaction txn = new Transaction(111, IsolationLevel.READ_COMMITTED);
    bpt.insert(key, rid, txn);
    BPlusTreePage bptp = new BPlusTreePage(bufferPoolManager.fetchPage(bpt.getRootPageId()), IntType.INSTANCE);
    assert bptp.getPageType() == BPlusTreePage.BTNodeType.LEAF;
    LeafPage lp = new LeafPage(bptp, IntType.INSTANCE);
    assert lp.getCurrentSize() == 1;
    assert lp.getRID(0).equals(rid);
    assert lp.getKey(0).equals(key);
  }

  @Test
  public void insertionTest2() throws Exception {
    List<Value<?, ?>> keys = new ArrayList<>();
    keys.add(new IntValue(42));
    keys.add(new IntValue(43));
    keys.add(new IntValue(44));
    keys.add(new IntValue(45));
    keys.add(new IntValue(46));

    List<RID> rids = new ArrayList<>();
    rids.add(new RID(42, 12122));
    rids.add(new RID(43, 12122));
    rids.add(new RID(44, 12122));
    rids.add(new RID(45, 12122));
    rids.add(new RID(46, 12122));

    BPlusTree bpt = new BPlusTree(bufferPoolManager, IntType.INSTANCE, 3, 4);
    Transaction txn = new Transaction(111, IsolationLevel.READ_COMMITTED);
    for (int i = 0; i < 5; i++) {
      bpt.insert(keys.get(i), rids.get(i), txn);
    }

    // use point search method
    for (int i = 0; i < 5; i++) {
      assert bpt.getValue(keys.get(i)).equals(rids.get(i));
    }

    // use range search method
    Iterator<RID> iter = bpt.iterator(new IntValue(42));
    int i = 0;
    while (iter.hasNext()) {
      assert iter.next().equals(rids.get(i));
      i++;
    }

    // use range search method, start at index = 2
    iter = bpt.iterator(new IntValue(44));
    i = 2;
    while (iter.hasNext()) {
      assert iter.next().equals(rids.get(i));
      i++;
    }
  }

  // delete
  @Test
  public void deletionTest() throws Exception {
    List<Value<?, ?>> keys = new ArrayList<>();
    keys.add(new IntValue(42));
    keys.add(new IntValue(43));
    keys.add(new IntValue(44));
    keys.add(new IntValue(45));
    keys.add(new IntValue(46));

    List<RID> rids = new ArrayList<>();
    rids.add(new RID(42, 12122));
    rids.add(new RID(43, 12122));
    rids.add(new RID(44, 12122));
    rids.add(new RID(45, 12122));
    rids.add(new RID(46, 12122));

    BPlusTree bpt = new BPlusTree(bufferPoolManager, IntType.INSTANCE, 3, 4);
    Transaction txn = new Transaction(111, IsolationLevel.READ_COMMITTED);
    for (int i = 0; i < 5; i++) {
      bpt.insert(keys.get(i), rids.get(i), txn);
    }

    // delete 42
    bpt.remove(keys.get(0), txn);
    assert bpt.getValue(keys.get(0)) == null;
    assert bpt.getValue(keys.get(1)).equals(rids.get(1));
    assert bpt.getValue(keys.get(2)).equals(rids.get(2));
    assert bpt.getValue(keys.get(3)).equals(rids.get(3));
    assert bpt.getValue(keys.get(4)).equals(rids.get(4));

    // delete 46
    bpt.remove(keys.get(4), txn);
    assert bpt.getValue(keys.get(0)) == null;
    assert bpt.getValue(keys.get(1)).equals(rids.get(1));
    assert bpt.getValue(keys.get(2)).equals(rids.get(2));
    assert bpt.getValue(keys.get(3)).equals(rids.get(3));
    assert bpt.getValue(keys.get(4)) == null;

    // delete 44
    System.out.println(bpt.toJson());
    bpt.remove(keys.get(2), txn);
    assert bpt.getValue(keys.get(0)) == null;
    assert bpt.getValue(keys.get(1)).equals(rids.get(1));
    assert bpt.getValue(keys.get(2)) == null;
    assert bpt.getValue(keys.get(3)).equals(rids.get(3));
    assert bpt.getValue(keys.get(4)) == null;

    // delete 43
    bpt.remove(keys.get(1), txn);
    assert bpt.getValue(keys.get(0)) == null;
    assert bpt.getValue(keys.get(1)) == null;
    assert bpt.getValue(keys.get(2)) == null;
    assert bpt.getValue(keys.get(3)).equals(rids.get(3));
    assert bpt.getValue(keys.get(4)) == null;

    // delete 45
    bpt.remove(keys.get(3), txn);
    assert bpt.getValue(keys.get(0)) == null;
    assert bpt.getValue(keys.get(1)) == null;
    assert bpt.getValue(keys.get(2)) == null;
    assert bpt.getValue(keys.get(3)) == null;
    assert bpt.getValue(keys.get(4)) == null;
  }

  // ===----------------===
  // concurrency test
  // ===----------------===

  @Test
  public void concurrencyInsTest() throws Exception {
    int numOfThreads = 100;
    ExecutorService conExec  = Executors.newFixedThreadPool(numOfThreads);
    // this is not the latch in DB world.
    CountDownLatch latch = new CountDownLatch(numOfThreads);
    BPlusTree bpt = new BPlusTree(bufferPoolManager, IntType.INSTANCE);
    for (int i = 0; i < numOfThreads; i++) {
      int finalI = i;
      Transaction txn = new Transaction(finalI, IsolationLevel.READ_COMMITTED);
      conExec.execute(
        () -> {
          try {
            bpt.insert(new IntValue(finalI), new RID(finalI, finalI), txn);
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
          latch.countDown();
        }
      );
    }
    latch.await();
    System.out.println(bpt.toJson());
  }

  // after test, delete the test.db file
  @After
  public void tearDown() throws Exception {
    java.nio.file.Files.delete(Paths.get("test.db"));
  }
}
