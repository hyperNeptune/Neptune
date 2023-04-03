package cn.edu.thssdb.type;

import cn.edu.thssdb.storage.*;
import cn.edu.thssdb.utils.Global;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.file.Paths;

import static org.junit.Assert.assertEquals;

public class ValueTest {
  private DiskManager diskManager;
  private Page page;

  @Before
  public void setUp() throws Exception {
    diskManager = new DiskManager(Paths.get("test.db"));
    page = new Page(0);
  }

  // new couple of values, write them to page, and read in, check if they are equal
  @Test
  public void testValue() throws Exception {
    // new int value array, PAGE_SIZE / INT_SIZE values
    int intSize = Global.INT_SIZE;
    int intNum = Global.PAGE_SIZE / intSize;
    Value[] intValues = new Value[intNum];
    for (int i = 0; i < intNum; i++) {
      intValues[i] = new IntValue((int) (Math.random() * 100000));
    }
    // write to a page
    for (int i = 0; i < intNum; i++) {
      intValues[i].serialize(page.GetData(), i * intSize);
    }
    diskManager.writePage(0, page);
    assertEquals(0, page.GetPageId());
    // new page
    Page page2 = new Page(0);
    diskManager.readPage(0, page2);
    assertEquals(0, page2.GetPageId());
    // read in
    Value[] intValues2 = new Value[intNum];
    for (int i = 0; i < intNum; i++) {
      intValues2[i] = new IntValue(page2.GetData().getInt(i * intSize));
    }
    // check if they are equal
    for (int i = 0; i < intNum; i++) {
      assertEquals(intValues[i].compareTo(intValues2[i]), 0);
    }
  }

  // after test, delete the test.db file
  @After
  public void tearDown() throws Exception {
    java.nio.file.Files.delete(Paths.get("test.db"));
  }
}
