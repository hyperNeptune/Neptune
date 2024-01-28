package neptune.storage;

import neptune.backend.storage.DiskManager;
import neptune.backend.storage.Page;
import neptune.common.Global;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.file.Paths;

import static org.junit.Assert.assertEquals;

public class DiskManagerTest {
  private DiskManager diskManager;
  private Page page;

  @Before
  public void setUp() throws Exception {
    diskManager = new DiskManager(Paths.get("test.db"));
    page = new Page(0);
  }

  @Test
  public void testRWPage() throws Exception {
    // random generate PAGE_SIZE bytes
    // write them to page, and read in, check if they are equal
    byte[] bytes = new byte[Global.PAGE_SIZE];
    for (int i = 4; i < Global.PAGE_SIZE; i++) {
      bytes[i] = (byte) (Math.random() * 256);
    }
    page.setData(bytes);
    diskManager.writePage(0, page);
    page.setPageId(0);
    assertEquals(0, page.getPageId());
    // new page
    Page page2 = new Page(0);
    diskManager.readPage(0, page2);
    assertEquals(0, page2.getPageId());
    byte[] bytes2 = page2.getData().array();
    for (int i = 0; i < Global.PAGE_SIZE; i++) {
      assertEquals(bytes[i], bytes2[i]);
    }
  }

  // after test, delete the test.db file
  @After
  public void tearDown() throws Exception {
    java.nio.file.Files.delete(Paths.get("test.db"));
  }
}
