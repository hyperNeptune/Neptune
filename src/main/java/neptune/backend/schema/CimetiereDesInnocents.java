package neptune.backend.schema;

import neptune.backend.buffer.BufferPoolManager;
import neptune.backend.storage.Tuple;
import neptune.backend.type.*;
import neptune.common.Pair;
import neptune.common.RID;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

// Cimetiere Des Innocents is a cemetery in Paris, France historically,
// here this is a table of all databases, and has no relation with its original meaning.
// TODO: Test this class
public class CimetiereDesInnocents {
  private final BufferPoolManager bufferPoolManager_;
  private final Map<String, Catalog> databases_;
  // 元神页
  private final Table metaKamiTable_;
  private static final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
  private static final Schema metaKamiSchema_ =
      new Schema(
          new Column[] {
            new Column("database_name", StringType.getVarCharType(50), (byte) 1, (byte) 0, 50, 0),
            new Column("database_catalog_pageid", IntType.INSTANCE, (byte) 0, (byte) 0, 4, 50)
          });
  public static final int META_KAMI_PAGE_ID = 0;

  private CimetiereDesInnocents(BufferPoolManager bpm, Table metaKamiTable) {
    bufferPoolManager_ = bpm;
    databases_ = new HashMap<>();
    metaKamiTable_ = metaKamiTable;
  }

  public static CimetiereDesInnocents createCDI(BufferPoolManager bpm) throws Exception {
    Table metaKamiTable = SlotTable.newSlotTable(bpm, metaKamiSchema_.getTupleSize());
    return new CimetiereDesInnocents(bpm, metaKamiTable);
  }

  public static CimetiereDesInnocents openCDI(BufferPoolManager bpm) throws Exception {
    Table metaKamiTable = SlotTable.openSlotTable(bpm, META_KAMI_PAGE_ID);
    CimetiereDesInnocents cdi = new CimetiereDesInnocents(bpm, metaKamiTable);
    cdi.reload();
    return cdi;
  }

  public void reload() throws Exception {
    Iterator<Pair<Tuple, RID>> iter = metaKamiTable_.iterator();
    while (iter.hasNext()) {
      Pair<Tuple, RID> pair = iter.next();
      Tuple tuple = pair.left;
      String databaseName = (String) tuple.getValue(metaKamiSchema_, 0).getValue();
      int databaseCatalogPageId = (Integer) tuple.getValue(metaKamiSchema_, 1).getValue();
      Catalog catalog = Catalog.loadCatalog(bufferPoolManager_, databaseCatalogPageId);
      databases_.put(databaseName, catalog);
    }
  }

  public Catalog createDatabase(String name) throws Exception {
    Catalog catalog = Catalog.createCatalog(bufferPoolManager_);
    databases_.put(name, catalog);
    metaKamiTable_.insert(
        new Tuple(
            new Value[] {new StringValue(name, 50), new IntValue(catalog.getFirstPageId())},
            metaKamiSchema_),
        null);
    return catalog;
  }

  public void dropDatabase(String tableName) throws Exception {
    Catalog catalog = databases_.get(tableName);
    if (catalog == null) {
      return;
    }
    // iterate
    Iterator<Pair<Tuple, RID>> iter = metaKamiTable_.iterator();
    while (iter.hasNext()) {
      Pair<Tuple, RID> pair = iter.next();
      Tuple tuple = pair.left;
      String databaseName = (String) tuple.getValue(metaKamiSchema_, 0).getValue();
      if (databaseName.equals(tableName)) {
        iter.remove();
        databases_.remove(tableName);
        break;
      }
    }
  }

  public Catalog useDatabase(String dbname) {
    return databases_.get(dbname);
  }

  public String[] listDatabases() {
    return databases_.keySet().toArray(new String[0]);
  }

  public boolean hasDatabase(String dbName) {
    return databases_.containsKey(dbName);
  }
}
