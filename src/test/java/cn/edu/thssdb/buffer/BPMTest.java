package cn.edu.thssdb.buffer;

import cn.edu.thssdb.storage.DiskManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.file.Paths;

public class BPMTest {
  DiskManager diskManager;
  BufferPoolManager bufferPoolManager;
  ReplaceAlgorithm replacer;

  @Before
  public void setUp() throws Exception {
    diskManager = new DiskManager(Paths.get("test.db"));
    replacer = new LRUReplacer(10);
    bufferPoolManager = new BufferPoolManager(10, diskManager, replacer);
  }

  @Test
  public void testFetchPage() throws Exception {}

  @After
  public void tearDown() throws Exception {
    java.nio.file.Files.delete(Paths.get("test.db"));
  }
}
