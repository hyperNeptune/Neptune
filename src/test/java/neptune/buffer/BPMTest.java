package neptune.buffer;

import neptune.storage.DiskManager;
import neptune.storage.Page;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.file.Paths;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class BPMTest {
  DiskManager diskManager;
  BufferPoolManager bufferPoolManager;
  ReplaceAlgorithm replacer;
  Page p;

  @Before
  public void setUp() throws Exception {
    diskManager = new DiskManager(Paths.get("test.db"));
    replacer = new LRUReplacer(10);
    bufferPoolManager = new BufferPoolManager(10, diskManager, replacer);
  }

  @Test
  public void testNewPage() throws Exception {
    p = bufferPoolManager.newPage();
    assertEquals(0, p.getPageId());
    assertEquals(1, bufferPoolManager.getPoolSize());
    p.getData().putInt(20, 12345);
    assertEquals(12345, p.getData().getInt(20));

    for (int i = 1; i < 10; i++) {
      assertEquals(i, bufferPoolManager.newPage().getPageId());
    }
    assertEquals(10, bufferPoolManager.getPoolSize());
    // assert that the buffer pool is full
    assertNull(bufferPoolManager.newPage());

    bufferPoolManager.unpinPage(0, true);
    bufferPoolManager.unpinPage(1, true);
    bufferPoolManager.unpinPage(2, true);
    bufferPoolManager.unpinPage(3, true);
    bufferPoolManager.unpinPage(4, true);
    for (int i = 0; i < 4; i++) {
      bufferPoolManager.newPage();
    }
    assertEquals(10, bufferPoolManager.getPoolSize());
    p = bufferPoolManager.fetchPage(0);
    bufferPoolManager.unpinPage(0, true);
    assertEquals(0, p.getPageId());
    assertEquals(12345, p.getData().getInt(20));

    bufferPoolManager.newPage();
    bufferPoolManager.newPage();
    bufferPoolManager.newPage();
    bufferPoolManager.newPage();
    assertNull(bufferPoolManager.fetchPage(0));
  }

  @After
  public void tearDown() throws Exception {
    java.nio.file.Files.delete(Paths.get("test.db"));
  }
}
