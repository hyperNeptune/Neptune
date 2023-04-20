package cn.edu.thssdb.type;

import cn.edu.thssdb.schema.Column;
import cn.edu.thssdb.schema.Schema;
import cn.edu.thssdb.storage.DiskManager;
import cn.edu.thssdb.storage.Page;
import cn.edu.thssdb.utils.Global;
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
        IntType INT_TYPE = new IntType();
        Column col1 = new Column("col1", INT_TYPE, (byte) 0,(byte) 0, 10, 0);
        Column col2 = new Column("col2", INT_TYPE, (byte) 0,(byte) 0, 10, 0);
        Column[] columns = new Column[]{col1, col2};
        Schema schema = new Schema(columns);
        schema.serialize(page.getData(),0);
        diskManager.writePage(0, page);

        Page new_page = new Page(0);
        diskManager.readPage(0, new_page);
        Schema new_schema = Schema.deserialize(new_page.getData(), 0).getFirst();
        System.out.println(new_schema.getColumn(0).getName());

    }

    // after test, delete the test.db file
    @After
    public void tearDown() throws Exception {
        java.nio.file.Files.delete(Paths.get("test.db"));
    }
}
