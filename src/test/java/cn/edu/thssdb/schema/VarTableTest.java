package cn.edu.thssdb.schema;

import cn.edu.thssdb.buffer.BufferPoolManager;
import cn.edu.thssdb.buffer.LRUReplacer;
import cn.edu.thssdb.buffer.ReplaceAlgorithm;
import cn.edu.thssdb.storage.DiskManager;
import cn.edu.thssdb.storage.Tuple;
import cn.edu.thssdb.type.*;
import cn.edu.thssdb.utils.Global;
import cn.edu.thssdb.utils.Pair;
import cn.edu.thssdb.utils.RID;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.util.Iterator;

import static org.junit.Assert.assertEquals;

public class VarTableTest {
  private DiskManager diskManager;
  private BufferPoolManager bufferPoolManager;
  private ReplaceAlgorithm replaceAlgorithm;
  private Table table;

  @Before
  public void setUp() throws Exception {
    replaceAlgorithm = new LRUReplacer(Global.DEFAULT_BUFFER_SIZE);
    diskManager = new DiskManager(Paths.get("test.db"));
    bufferPoolManager =
        new BufferPoolManager(Global.DEFAULT_BUFFER_SIZE, diskManager, replaceAlgorithm);
  }

  @Test
  public void testTablePage() throws Exception {
    table = VarTable.newVarTable(bufferPoolManager);
    // a string array
    String[] s = new String[5];
    // use the title of famous books for it's string
    s[0] = "The Lord of the Rings tower of twins";
    s[1] = "The Little Prince is a french famous";
    s[2] = "Harry Potter and the Philosopher's Stone";
    s[3] = "And Then There Were None .............";
    s[4] = "Dream of the Red Chamber in china book";

    // create byte buffers
    ByteBuffer[] byteBuffers = new ByteBuffer[5];
    for (int i = 0; i < 5; i++) {
      byteBuffers[i] = ByteBuffer.allocate(s[i].length());
      byteBuffers[i].put(s[i].getBytes());
      byteBuffers[i].clear();
    }

    // make tuples
    Tuple[] tuples = new Tuple[5];
    for (int i = 0; i < 5; i++) {
      tuples[i] = new Tuple(byteBuffers[i]);
    }

    // insert tuples
    for (int i = 0; i < 1000; i++) {
      table.insert(tuples[i % 5], null);
    }
    bufferPoolManager.flushAllPages();
    // use iterator
    Iterator<Pair<Tuple, RID>> iterator = table.iterator();
    int i = 0;
    while (iterator.hasNext()) {
      Tuple tuple = iterator.next().left;
      assertEquals(s[i % 5], tuple.toString());
      i++;
    }
  }

  @Test
  public void testTableIterator() throws Exception {}

  // after test, delete the test.db file
  @After
  public void tearDown() throws Exception {
    java.nio.file.Files.delete(Paths.get("test.db"));
  }
}
