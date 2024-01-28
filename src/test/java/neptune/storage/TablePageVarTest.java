package neptune.storage;

import neptune.buffer.BufferPoolManager;
import neptune.buffer.LRUReplacer;
import neptune.buffer.ReplaceAlgorithm;
import neptune.schema.*;
import neptune.type.*;
import neptune.utils.Pair;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.util.Iterator;

import static org.junit.Assert.assertEquals;

public class TablePageVarTest {
  private DiskManager diskManager;
  private BufferPoolManager bufferPoolManager;
  private ReplaceAlgorithm replaceAlgorithm;

  @Before
  public void setUp() throws Exception {
    diskManager = new DiskManager(Paths.get("test.db"));
    replaceAlgorithm = new LRUReplacer(10);
    bufferPoolManager = new BufferPoolManager(10, diskManager, replaceAlgorithm);
  }

  @Test
  public void testTablePage() throws Exception {
    String s = "hello world";
    String s2 = "four scores and seven years ago our fathers, brought forth this continent";
    Tuple a = new Tuple(ByteBuffer.wrap(s.getBytes()));
    Tuple b = new Tuple(ByteBuffer.wrap(s2.getBytes()));
    TablePageVar tablePage = new TablePageVar(bufferPoolManager.newPage());
    tablePage.init(null);
    for (int i = 0; i < 100; i++) {
      tablePage.insertTuple(a, null);
      tablePage.insertTuple(b, null);
    }
    tablePage.printmeta();
    // iterator
    Iterator<Pair<Tuple, Integer>> iterator = tablePage.iterator();
    while (iterator.hasNext()) {
      Tuple tuple = iterator.next().left;
      System.out.println(tuple.toString());
    }
    // delete
    tablePage.deleteTuple(73);
    assertEquals(73, tablePage.insertTuple(a, null));
    assertEquals("hello world", tablePage.getTuple(73).toString());
  }

  // after test, delete the test.db file
  @After
  public void tearDown() throws Exception {
    java.nio.file.Files.delete(Paths.get("test.db"));
  }
}
