package neptune.type;

import neptune.backend.schema.Column;
import neptune.backend.schema.Schema;
import neptune.backend.storage.DiskManager;
import neptune.backend.storage.Page;
import neptune.backend.type.IntType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.file.Paths;

import static org.junit.Assert.assertEquals;

public class SchemaTest {
  private DiskManager diskManager;
  private Page page;

  @Before
  public void setUp() throws Exception {
    diskManager = new DiskManager(Paths.get("test.db"));
    page = new Page(0);
  }

  @Test
  public void testSchema() throws Exception {
    page = new Page(5);
    Column col1 = new Column("col1", IntType.INSTANCE, (byte) 0, (byte) 0, 64, 0);
    Column col2 = new Column("col2", IntType.INSTANCE, (byte) 0, (byte) 0, 64, 0);
    Column[] columns = new Column[] {col1, col2};
    Schema schema = new Schema(columns);
    schema.serialize(page.getData(), 0);
    diskManager.writePage(5, page);

    Page new_page = new Page(5);
    diskManager.readPage(5, new_page);
    Schema new_schema = Schema.deserialize(new_page.getData(), 0).left;
    assertEquals("col1", new_schema.getColumn(0).getName());
    assertEquals("col2", new_schema.getColumn(1).getName());
  }

  // after test, delete the test.db file
  @After
  public void tearDown() throws Exception {
    java.nio.file.Files.delete(Paths.get("test.db"));
  }
}
