package neptune.storage.index;

import neptune.backend.buffer.BufferPoolManager;
import neptune.backend.buffer.LRUReplacer;
import neptune.backend.buffer.ReplaceAlgorithm;
import neptune.backend.concurrency.IsolationLevel;
import neptune.backend.concurrency.Transaction;
import neptune.backend.storage.DiskManager;
import neptune.backend.storage.index.BPlusTree;
import neptune.backend.storage.index.BPlusTreePage;
import neptune.backend.storage.index.LeafPage;
import neptune.backend.type.IntType;
import neptune.backend.type.IntValue;
import neptune.backend.type.Value;
import neptune.common.Global;
import neptune.common.Pair;
import neptune.common.RID;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
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
    Iterator<Pair<Value<?, ?>, RID>> iter = bpt.iterator(new IntValue(51));
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
    BPlusTreePage bptp =
        new BPlusTreePage(bufferPoolManager.fetchPage(bpt.getRootPageId()), IntType.INSTANCE);
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
    Iterator<Pair<Value<?, ?>, RID>> iter = bpt.iterator(new IntValue(42));
    int i = 0;
    while (iter.hasNext()) {
      assert iter.next().right.equals(rids.get(i));
      i++;
    }

    // use range search method, start at index = 2
    iter = bpt.iterator(new IntValue(44));
    i = 2;
    while (iter.hasNext()) {
      assert iter.next().right.equals(rids.get(i));
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
  public void concurrencyTest() throws Exception {
    int numOfThreads = 1000;
    ExecutorService conExec = Executors.newFixedThreadPool(numOfThreads);
    // this is not the latch in DB world.
    CountDownLatch latch = new CountDownLatch(numOfThreads);
    BPlusTree bpt = new BPlusTree(bufferPoolManager, IntType.INSTANCE, 3, 4);
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
          });
    }
    latch.await();
    for (int i = 0; i < numOfThreads; i++) {
      assert bpt.getValue(new IntValue(i)).equals(new RID(i, i));
    }

    // concurrent delete
    CountDownLatch latch2 = new CountDownLatch(numOfThreads);
    for (int i = 0; i < numOfThreads; i++) {
      int finalI = i;
      Transaction txn = new Transaction(finalI, IsolationLevel.READ_COMMITTED);
      conExec.execute(
          () -> {
            try {
              bpt.remove(new IntValue(finalI), txn);
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
            latch2.countDown();
          });
    }
    latch2.await();
    System.out.println(bpt.toJson());
    for (int i = 0; i < numOfThreads; i++) {
      assert bpt.getValue(new IntValue(i)) == null;
    }

    // concurrent insert 0 ~ 100 again
    CountDownLatch latch3 = new CountDownLatch(numOfThreads);
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
            latch3.countDown();
          });
    }
    latch3.await();
    for (int i = 0; i < numOfThreads; i++) {
      assert bpt.getValue(new IntValue(i)).equals(new RID(i, i));
    }

    // interleave insert 100 ~ 200, and delete 0 ~ 100
    CountDownLatch latch4 = new CountDownLatch(numOfThreads);
    for (int i = 0; i < numOfThreads; i++) {
      int finalI = i;
      Transaction txn = new Transaction(finalI, IsolationLevel.READ_COMMITTED);
      conExec.execute(
          () -> {
            try {
              bpt.insert(
                  new IntValue(finalI + numOfThreads),
                  new RID(finalI + numOfThreads, finalI + numOfThreads),
                  txn);
              bpt.remove(new IntValue(finalI), txn);
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
            latch4.countDown();
          });
    }
    latch4.await();
    for (int i = 0; i < numOfThreads; i++) {
      assert bpt.getValue(new IntValue(i)) == null;
      assert bpt.getValue(new IntValue(i + numOfThreads))
          .equals(new RID(i + numOfThreads, i + numOfThreads));
    }
  }

  // rewrite the concurrency test above to single thread mode
  @Test
  public void serialTest2() throws Exception {
    int numOfThreads = 1000;
    BPlusTree bpt = new BPlusTree(bufferPoolManager, IntType.INSTANCE, 3, 4);
    for (int i = 0; i < numOfThreads; i++) {
      Transaction txn = new Transaction(i, IsolationLevel.READ_COMMITTED);
      bpt.insert(new IntValue(i), new RID(i, i), txn);
    }
    for (int i = 0; i < numOfThreads; i++) {
      assert bpt.getValue(new IntValue(i)).equals(new RID(i, i));
    }

    // concurrent delete
    for (int i = 0; i < numOfThreads; i++) {
      Transaction txn = new Transaction(i, IsolationLevel.READ_COMMITTED);
      bpt.remove(new IntValue(i), txn);
    }
    for (int i = 0; i < numOfThreads; i++) {
      assert bpt.getValue(new IntValue(i)) == null;
    }

    // concurrent insert 0 ~ 100 again
    for (int i = 0; i < numOfThreads; i++) {
      Transaction txn = new Transaction(i, IsolationLevel.READ_COMMITTED);
      bpt.insert(new IntValue(i), new RID(i, i), txn);
    }

    for (int i = 0; i < numOfThreads; i++) {
      assert bpt.getValue(new IntValue(i)).equals(new RID(i, i));
    }

    // interleave insert 100 ~ 200, and delete 0 ~ 100
    for (int i = 0; i < numOfThreads; i++) {
      Transaction txn = new Transaction(i, IsolationLevel.READ_COMMITTED);
      bpt.insert(new IntValue(i + numOfThreads), new RID(i + numOfThreads, i + numOfThreads), txn);
      bpt.remove(new IntValue(i), txn);
    }
    for (int i = 0; i < numOfThreads; i++) {
      assert bpt.getValue(new IntValue(i)) == null;
      assert bpt.getValue(new IntValue(i + numOfThreads))
          .equals(new RID(i + numOfThreads, i + numOfThreads));
    }
  }

  @Test
  public void insDelTest() throws Exception {
    BPlusTree bpt = new BPlusTree(bufferPoolManager, IntType.INSTANCE, 3, 4);
    // insert 138 ~ 155
    for (int i = 138; i <= 155; i++) {
      Transaction txn = new Transaction(i, IsolationLevel.READ_COMMITTED);
      bpt.insert(new IntValue(i), new RID(i, i), txn);
    }
    // delete 148~153 at random order, print the order
    List<Integer> list = new ArrayList<>();
    for (int i = 148; i <= 153; i++) {
      list.add(i);
    }
    Collections.shuffle(list);
    System.out.println(list);
    for (int i : list) {
      Transaction txn = new Transaction(i, IsolationLevel.READ_COMMITTED);
      bpt.remove(new IntValue(i), txn);
    }
    System.out.println(bpt.toJson());
  }

  @Test
  public void insDel2Test() throws Exception {
    BPlusTree bpt = new BPlusTree(bufferPoolManager, IntType.INSTANCE, 3, 4);
    // first insert 0 ~ 100
    for (int i = 0; i < 100; i++) {
      Transaction txn = new Transaction(i, IsolationLevel.READ_COMMITTED);
      bpt.insert(new IntValue(i), new RID(i, i), txn);
    }

    // remove
    for (int i = 0; i < 100; i++) {
      Transaction txn = new Transaction(i, IsolationLevel.READ_COMMITTED);
      bpt.remove(new IntValue(i), txn);
    }

    // insert 0 ~ 100 again
    for (int i = 0; i < 100; i++) {
      Transaction txn = new Transaction(i, IsolationLevel.READ_COMMITTED);
      bpt.insert(new IntValue(i), new RID(i, i), txn);
    }
    // do the sequence below:
    //    inserting 100
    //    inserted 100
    //    inserting 104
    //    inserting 120
    //    inserted 104
    //    inserted 120
    //    inserting 131
    //    inserted 131
    //    inserting 101
    //    inserted 101
    //    removing 1
    //    removed 1
    //    inserting 143
    //    inserted 143
    //    inserting 102
    //    inserted 102
    //    removing 2
    //    removed 2
    //    inserting 154
    //    inserted 154
    //    inserting 169
    //    inserted 169
    //    inserting 178
    //    inserted 178
    //    inserting 182
    //    inserted 182
    //    inserting 188
    //    inserted 188
    //    inserting 103
    //    inserted 103
    //    removing 3
    //    removed 3
    //    removing 0
    //    removed 0
    //    inserting 105
    //    inserted 105
    //    removing 5
    //    removed 5
    //    inserting 106
    //    inserted 106
    //    removing 6
    //    removed 6
    //    inserting 107
    //    inserted 107
    //    removing 7
    //    removed 7
    //    inserting 108
    //    inserted 108
    //    removing 8
    //    removed 8
    //    inserting 109
    //    inserted 109
    //    removing 9
    //    removed 9
    //    inserting 110
    //    inserted 110
    //    removing 10
    //    removed 10
    //    inserting 111
    //    inserted 111
    //    removing 11
    //    removed 11
    //    inserting 112
    //    inserted 112
    //    inserting 113
    //    inserted 113
    //    removing 13
    //    removed 13
    //    inserting 114
    //    inserted 114
    //    removing 14
    //    removed 14
    //    inserting 115
    //    inserted 115
    //    removing 15
    //    removed 15
    //    inserting 116
    //    inserted 116
    //    inserting 117
    //    inserted 117
    //    removing 17
    //    removed 17
    //    inserting 118
    //    inserted 118
    //    removing 18
    //    removed 18
    //    inserting 119
    //    inserted 119
    //    inserting 121
    //    inserted 121
    //    inserting 122
    //    inserted 122
    //    removing 22
    //    removed 22
    //    removing 4
    //    removed 4
    //    inserting 123
    //    inserted 123
    //    inserting 124
    //    inserted 124
    //    removing 24
    //    removed 24
    //    inserting 125
    //    inserted 125
    //    removing 25
    //    removed 25
    //    inserting 126
    //    inserted 126
    //    removing 26
    //    removed 26
    //    inserting 127
    //    inserted 127
    //    removing 27
    //    removed 27
    //    inserting 128
    //    inserted 128
    //    removing 28
    // start, no println
    Transaction txn = new Transaction(100, IsolationLevel.READ_COMMITTED);
    bpt.insert(new IntValue(100), new RID(100, 100), txn);
    bpt.insert(new IntValue(104), new RID(104, 104), txn);
    bpt.insert(new IntValue(120), new RID(120, 120), txn);
    bpt.insert(new IntValue(131), new RID(131, 131), txn);
    bpt.insert(new IntValue(101), new RID(101, 101), txn);
    bpt.remove(new IntValue(1), txn);
    bpt.insert(new IntValue(143), new RID(143, 143), txn);
    bpt.insert(new IntValue(102), new RID(102, 102), txn);
    bpt.remove(new IntValue(2), txn);
    bpt.insert(new IntValue(154), new RID(154, 154), txn);
    bpt.insert(new IntValue(169), new RID(169, 169), txn);
    bpt.insert(new IntValue(178), new RID(178, 178), txn);
    bpt.insert(new IntValue(182), new RID(182, 182), txn);
    bpt.insert(new IntValue(188), new RID(188, 188), txn);
    bpt.insert(new IntValue(103), new RID(103, 103), txn);
    bpt.remove(new IntValue(3), txn);
    bpt.remove(new IntValue(0), txn);
    bpt.insert(new IntValue(105), new RID(105, 105), txn);
    bpt.remove(new IntValue(5), txn);
    bpt.insert(new IntValue(106), new RID(106, 106), txn);
    bpt.remove(new IntValue(6), txn);
    bpt.insert(new IntValue(107), new RID(107, 107), txn);
    bpt.remove(new IntValue(7), txn);
    bpt.insert(new IntValue(108), new RID(108, 108), txn);
    bpt.remove(new IntValue(8), txn);
    bpt.insert(new IntValue(109), new RID(109, 109), txn);
    bpt.remove(new IntValue(9), txn);
    bpt.insert(new IntValue(110), new RID(110, 110), txn);
    bpt.remove(new IntValue(10), txn);
    bpt.insert(new IntValue(111), new RID(111, 111), txn);
    bpt.remove(new IntValue(11), txn);
    bpt.insert(new IntValue(112), new RID(112, 112), txn);
    bpt.insert(new IntValue(113), new RID(113, 113), txn);
    bpt.remove(new IntValue(13), txn);
    bpt.insert(new IntValue(114), new RID(114, 114), txn);
    bpt.remove(new IntValue(14), txn);
    bpt.insert(new IntValue(115), new RID(115, 115), txn);
    bpt.remove(new IntValue(15), txn);
    bpt.insert(new IntValue(116), new RID(116, 116), txn);
    bpt.insert(new IntValue(117), new RID(117, 117), txn);
    bpt.remove(new IntValue(17), txn);
    bpt.insert(new IntValue(118), new RID(118, 118), txn);
    bpt.remove(new IntValue(18), txn);
    bpt.insert(new IntValue(119), new RID(119, 119), txn);
    bpt.insert(new IntValue(121), new RID(121, 121), txn);
    bpt.insert(new IntValue(122), new RID(122, 122), txn);
    bpt.remove(new IntValue(22), txn);
    bpt.remove(new IntValue(4), txn);
    bpt.insert(new IntValue(123), new RID(123, 123), txn);
    bpt.insert(new IntValue(124), new RID(124, 124), txn);
    bpt.remove(new IntValue(24), txn);
    bpt.insert(new IntValue(125), new RID(125, 125), txn);
    bpt.remove(new IntValue(25), txn);
    bpt.insert(new IntValue(126), new RID(126, 126), txn);
    bpt.remove(new IntValue(26), txn);
    bpt.insert(new IntValue(127), new RID(127, 127), txn);
    bpt.remove(new IntValue(27), txn);
    bpt.insert(new IntValue(128), new RID(128, 128), txn);
    bpt.remove(new IntValue(28), txn);
  }

  @Test
  public void borrowTest() throws Exception {
    BPlusTree bpt = new BPlusTree(bufferPoolManager, IntType.INSTANCE, 4, 5);
    // generate a test case where in the bottom of the tree
    // we have a node with 2 children, and the left child has 2 keys
    // and the right child has 1 key, delete a key from the right child
    // then it borrow a key from the left child
    // the tree should be like this:
    Transaction txn = new Transaction(1, IsolationLevel.READ_COMMITTED);
    bpt.insert(new IntValue(1), new RID(1, 1), txn);
    bpt.insert(new IntValue(2), new RID(2, 2), txn);
    bpt.insert(new IntValue(6), new RID(3, 3), txn);
    bpt.insert(new IntValue(8), new RID(4, 4), txn);
    bpt.insert(new IntValue(9), new RID(5, 5), txn);
    bpt.remove(new IntValue(1), txn);
    bpt.insert(new IntValue(7), new RID(5, 5), txn);
    bpt.remove(new IntValue(2), txn);
    System.out.println(bpt.toJson());
  }

  @Test
  public void borrowTest2() throws Exception {
    BPlusTree bpt = new BPlusTree(bufferPoolManager, IntType.INSTANCE, 4, 5);
    // generate a test case where in the bottom of the tree
    // we have a node with 2 children, and the left child has 2 keys
    // and the right child has 1 key, delete a key from the right child
    // then it borrow a key from the left child
    // the tree should be like this:
    Transaction txn = new Transaction(1, IsolationLevel.READ_COMMITTED);
    bpt.insert(new IntValue(1), new RID(1, 1), txn);
    bpt.insert(new IntValue(2), new RID(2, 2), txn);
    bpt.insert(new IntValue(8), new RID(3, 3), txn);
    bpt.insert(new IntValue(11), new RID(4, 4), txn);
    bpt.insert(new IntValue(3), new RID(5, 5), txn);
    System.out.println(bpt.toJson());
    bpt.remove(new IntValue(8), txn);
    System.out.println(bpt.toJson());
  }

  @Test
  public void borrowTest3() throws Exception {
    BPlusTree bpt = new BPlusTree(bufferPoolManager, IntType.INSTANCE, 3, 6);
    // generate a test case where in the bottom of the tree
    // we have a node with 2 children, and the left child has 2 keys
    // and the right child has 1 key, delete a key from the right child
    // then it borrow a key from the left child
    // the tree should be like this:
    Transaction txn = new Transaction(1, IsolationLevel.READ_COMMITTED);
    for (int i = 1; i < 15; i++) {
      bpt.insert(new IntValue(i), new RID(i, i), txn);
    }
    bpt.remove(new IntValue(7), txn);
    System.out.println(bpt.toJson());
    bpt.remove(new IntValue(3), txn);
    System.out.println(bpt.toJson());
  }

  @Test
  public void borrowTest4() throws Exception {
    BPlusTree bpt = new BPlusTree(bufferPoolManager, IntType.INSTANCE, 3, 6);
    // generate a test case where in the bottom of the tree
    // we have a node with 2 children, and the left child has 2 keys
    // and the right child has 1 key, delete a key from the right child
    // then it borrow a key from the left child
    // the tree should be like this:
    Transaction txn = new Transaction(1, IsolationLevel.READ_COMMITTED);
    for (int i = 1; i < 15; i++) {
      bpt.insert(new IntValue(i), new RID(i, i), txn);
    }
    bpt.remove(new IntValue(7), txn);
    System.out.println(bpt.toJson());
    bpt.remove(new IntValue(12), txn);
    bpt.remove(new IntValue(13), txn);
    bpt.remove(new IntValue(14), txn);
    System.out.println(bpt.toJson());
  }

  // after test, delete the test.db file
  @After
  public void tearDown() throws Exception {
    java.nio.file.Files.delete(Paths.get("test.db"));
  }
}
