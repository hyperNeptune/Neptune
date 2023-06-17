package cn.edu.thssdb.schema;

import cn.edu.thssdb.buffer.BufferPoolManager;
import cn.edu.thssdb.storage.Tuple;
import cn.edu.thssdb.storage.index.BPlusTree;
import cn.edu.thssdb.utils.Pair;
import cn.edu.thssdb.utils.RID;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

// Catalog is used by executor for create, drop, lookup of tables as well as indices
public class Catalog {
  private final BufferPoolManager bufferPoolManager_;
  private Table catalogTable_;
  private Map<String, Table> tables_;
  private Map<String, Schema> tableSchemas_;
  private Map<String, BPlusTree> indices_;
  // use this strange name to avoid conflict with user's table name
  private static final String SCHEMA_TABLE_NAME = "__HYPERNEPTUNE__SAINTDENIS__SCHEMA__";
  private static final String SCHEMA_TABLE_SCHEMA =
      "CREATE TABLE "
          + SCHEMA_TABLE_NAME
          + " ("
          + "table_name TEXT NOT NULL PRIMARY KEY, "
          + "table_schema TEXT NOT NULL"
          + ");\n"
          + "Actually the schema above is a lie to you, 'cause I am a built-in table.\n";

  private Catalog(BufferPoolManager bpm) {
    bufferPoolManager_ = bpm;
  }

  // factory methods
  public static Catalog createCatalog(BufferPoolManager bpm) throws Exception {
    Catalog clg = new Catalog(bpm);
    Table t = VarTable.newVarTable(bpm);
    clg.catalogTable_ = t;
    clg.tables_ = new HashMap<>();
    clg.tableSchemas_ = new HashMap<>();
    clg.indices_ = new HashMap<>();
    clg.tables_.put(SCHEMA_TABLE_NAME, t);
    clg.tableSchemas_.put(SCHEMA_TABLE_NAME, null);
    clg.indices_.put(SCHEMA_TABLE_NAME, null);
    return clg;
  }

  public static Catalog loadCatalog(BufferPoolManager bpm, int catalogFirstPageId)
      throws Exception {
    Catalog clg = new Catalog(bpm);
    Table t = VarTable.openVarTable(bpm, catalogFirstPageId);
    clg.catalogTable_ = t;
    clg.tables_ = new HashMap<>();
    clg.tableSchemas_ = new HashMap<>();
    clg.tables_.put(SCHEMA_TABLE_NAME, t);
    clg.tableSchemas_.put(SCHEMA_TABLE_NAME, null);
    clg.indices_ = new HashMap<>();
    clg.reload();
    return clg;
  }

  // reload reads the catalog table and load all table information into memory
  public void reload() throws Exception {
    Iterator<Pair<Tuple, RID>> iterator = catalogTable_.iterator();
    while (iterator.hasNext()) {
      Tuple tuple = iterator.next().left;
      TableInfo ti = TableInfo.deserialize(tuple.getValue(), 0, bufferPoolManager_);
      tables_.put(ti.getTableName(), ti.getTable());
      tableSchemas_.put(ti.getTableName(), ti.getSchema());
      indices_.put(ti.getTableName(), ti.getIndex());
    }
  }

  public String[] list() {
    String[] ret = new String[tables_.size()];
    int i = 0;
    for (Map.Entry<String, Schema> entry : tableSchemas_.entrySet()) {
      if (entry.getKey().equals(SCHEMA_TABLE_NAME)) {
        ret[i] = SCHEMA_TABLE_SCHEMA;
      } else {
        ret[i] = entry.getKey() + " : " + entry.getValue().toString();
      }
      i++;
    }
    return ret;
  }

  public TableInfo createTable(String tableName, Schema schema) throws Exception {
    Table tableHeap = SlotTable.newSlotTable(bufferPoolManager_, schema.getTupleSize());
    TableInfo ti =
        new TableInfo(
            tableName,
            schema,
            tableHeap,
            new BPlusTree(bufferPoolManager_, schema.getPkColumn().getType()));
    tables_.put(tableName, tableHeap);
    tableSchemas_.put(tableName, schema);
    indices_.put(tableName, ti.getIndex());
    catalogTable_.insert(ti.serialize(), null);
    return ti;
  }

  public void dropTable(String tableName) throws Exception {
    Table ti = getTable(tableName);
    if (ti == null) {
      return;
    }
    tables_.remove(tableName);
    tableSchemas_.remove(tableName);
    indices_.remove(tableName);
    // iterate through table page and drop it
    Iterator<Pair<Tuple, RID>> iterator = catalogTable_.iterator();
    while (iterator.hasNext()) {
      Pair<Tuple, RID> pair = iterator.next();
      Tuple tuple = pair.left;
      TableInfo ti1 = TableInfo.deserialize(tuple.getValue(), 0, bufferPoolManager_);
      if (ti1.getTableName().equals(tableName)) {
        catalogTable_.delete(pair.right);
        break;
      }
    }
  }

  public Table getTable(String tableName) {
    return tables_.get(tableName);
  }

  public Schema getTableSchema(String tableName) {
    return tableSchemas_.get(tableName);
  }

  public BPlusTree getIndex(String tableName) {
    return indices_.get(tableName);
  }

  public TableInfo getTableInfo(String tableName) {
    Table table = getTable(tableName);
    Schema schema = getTableSchema(tableName);
    BPlusTree index = getIndex(tableName);
    if (table == null || schema == null) {
      return null;
    }
    return new TableInfo(tableName, schema, table, index);
  }

  public int getFirstPageId() {
    return catalogTable_.getFirstPageId();
  }
}
