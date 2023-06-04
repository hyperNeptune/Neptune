package cn.edu.thssdb.schema;

import cn.edu.thssdb.buffer.BufferPoolManager;
import cn.edu.thssdb.storage.Page;
import cn.edu.thssdb.storage.TablePage;
import cn.edu.thssdb.storage.TablePageVar;
import cn.edu.thssdb.utils.Global;

public class VarTable extends Table {
  public VarTable(BufferPoolManager bufferPoolManager, int firstPageId, OpenFlag flag)
      throws Exception {
    super(bufferPoolManager, firstPageId, flag);
  }

  public VarTable(BufferPoolManager bufferPoolManager) throws Exception {
    super(bufferPoolManager, 0, NewFlag.INSTANCE);
  }

  // factory
  public static VarTable newVarTable(BufferPoolManager bufferPoolManager) throws Exception {
    return new VarTable(bufferPoolManager);
  }

  public static VarTable openVarTable(BufferPoolManager bufferPoolManager, int firstPageId)
      throws Exception {
    return new VarTable(bufferPoolManager, firstPageId, OpenFlag.INSTANCE);
  }

  @Override
  protected TablePage newTablePage(Page p, int slotSize) {
    if (p == null) {
      return null;
    }
    TablePage tablePage = new TablePageVar(p);
    tablePage.init(null);
    return tablePage;
  }

  @Override
  protected TablePage fetchTablePage(Page p) {
    return new TablePageVar(p);
  }

  @Override
  protected int getPageMaxTupleSize() {
    return Global.PAGE_SIZE
        - TablePageVar.TABLE_PAGE_VAR_HEADER_SIZE
        - TablePageVar.SLOT_META_SIZE;
  }
}
