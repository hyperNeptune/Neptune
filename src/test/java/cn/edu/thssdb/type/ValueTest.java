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

    /* Simple tests for int calculation */
    IntValue test1 = new IntValue(10);
    IntValue test2 = new IntValue(20);
    IntValue add_test1 = (IntValue) test1.add(test2);
    IntValue add_test2 = (IntValue) test2.add(test1);
    IntValue mul_test = (IntValue) test1.mul(test2);
    IntValue sub_test = (IntValue) test1.sub(test2);
    Integer add_acc = 30;
    Integer mul_acc = 200;
    Integer sub_acc = -10;
    assertEquals(add_test1.getValue(), add_test2.getValue());
    assertEquals(add_test1.getValue(), add_acc);
    assertEquals(mul_test.getValue(), mul_acc);
    assertEquals(sub_test.getValue(), sub_acc);

    StringValue str1 = new StringValue("komeiji ", 30);
    StringValue str2 = new StringValue("koishi", 10);
    StringValue concat_test = (StringValue) str1.add(str2);
    Integer concat_acc_len = 40;
    String concat_acc_str = "komeiji koishi";
    assertEquals(concat_test.getValue(), concat_acc_str);
    //TODO: 这里要再确认一下相加后string的长度如何确定
    /*assertEquals(concat_test.getSize(), concat_acc_len.intValue());*/



/*    Value[] intValues = new Value[intNum];
    for (int i = 0; i < intNum; i++) {
      intValues[i] = new IntValue((int) (Math.random() * 100000));
    }
    // write to a page
    for (int i = 0; i < intNum; i++) {
      intValues[i].serialize(page.getData(), i * intSize);
    }
    diskManager.writePage(0, page);
    assertEquals(0, page.getPageId());
    // new page
    Page page2 = new Page(0);
    diskManager.readPage(0, page2);
    assertEquals(0, page2.getPageId());
    // read in
    Value[] intValues2 = new Value[intNum];
    for (int i = 0; i < intNum; i++) {
      intValues2[i] = new IntValue(page2.getData().getInt(i * intSize));
    }
    // check if they are equal
    for (int i = 0; i < intNum; i++) {
      assertEquals(intValues[i].compareTo(intValues2[i]), 0);
    }*/

  }

  @Test
  public void hahaha() {
    byte a = 0;
    System.out.println(Byte.toString(a));
  }

  @Test
  public void BufferTest() throws Exception {
    page = new Page(5);
    assertEquals(0, page.getData().position());
    page.getData().putInt(0, 5);
    assertEquals(0, page.getData().position());
    page.getData().putInt(4, 2);
    assertEquals(0, page.getData().position());
    diskManager.writePage(5, page);

    Page page2 = new Page(5);
    diskManager.readPage(5, page2);
    assertEquals(Global.PAGE_SIZE, page2.getData().limit());
    assertEquals(Global.PAGE_SIZE, page2.getData().position());
    assertEquals(2, page2.getData().getInt(4));
    assertEquals(5, page2.getData().getInt(0));
    assertEquals(Global.PAGE_SIZE, page2.getData().position());
  }

  // after test, delete the test.db file
  @After
  public void tearDown() throws Exception {
    java.nio.file.Files.delete(Paths.get("test.db"));
  }
}
