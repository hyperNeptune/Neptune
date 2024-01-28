package neptune.buffer;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class LRUTest {
  ReplaceAlgorithm replacer;

  @Before
  public void setUp() throws Exception {
    replacer = new LRUReplacer(10);
  }

  @Test
  public void testReadAccess() throws Exception {
    // add 10
    for (int i = 0; i < 10; i++) {
      replacer.recordAccess(i);
      replacer.unpin(i);
    }
    replacer.recordAccess(5);
    replacer.recordAccess(3);
    replacer.pin(0);

    // evict, assert
    assertEquals(1, replacer.getVictim());
    assertEquals(2, replacer.getVictim());
    assertEquals(4, replacer.getVictim());
    assertEquals(6, replacer.getVictim());
    assertEquals(7, replacer.getVictim());
    assertEquals(8, replacer.getVictim());
    assertEquals(9, replacer.getVictim());
    assertEquals(-1, replacer.getVictim());

    // unpin 0, 5, 3 assert
    replacer.unpin(0);
    replacer.unpin(5);
    replacer.unpin(3);
    assertEquals(0, replacer.getVictim());
    assertEquals(5, replacer.getVictim());
    assertEquals(3, replacer.getVictim());
    assertEquals(-1, replacer.getVictim());
  }

  @After
  public void tearDown() throws Exception {}
}
