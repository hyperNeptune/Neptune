package neptune.backend.concurrency;

import neptune.backend.schema.Table;
import neptune.common.RID;

class TableWriteRecord {
  public String table_name;
  public RID rid;
  public Table table_heap;
  public WType wtype;

  public TableWriteRecord(String table_name, RID rid, Table table_heap) {
    this.table_name = table_name;
    this.rid = rid;
    this.table_heap = table_heap;
  }
}
