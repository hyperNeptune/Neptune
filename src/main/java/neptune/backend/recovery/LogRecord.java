package neptune.backend.recovery;

import neptune.backend.concurrency.Transaction;
import neptune.backend.storage.Tuple;
import neptune.common.RID;

/**
 * For every write operation on the table page, you should write ahead a corresponding log record.
 *
 * <p>For EACH log record, HEADER is like (5 fields in common, 20 bytes in total).
 * --------------------------------------------- | size | LSN | transID | prevLSN | LogType |
 * --------------------------------------------- For insert type log record
 * --------------------------------------------------------------- | HEADER | tuple_rid | tuple_size
 * | tuple_data | --------------------------------------------------------------- For delete type
 * (including markdelete, rollbackdelete, applydelete)
 * ---------------------------------------------------------------- | HEADER | tuple_rid |
 * tuple_size | tuple_data | --------------------------------------------------------------- For
 * update type log record
 * ----------------------------------------------------------------------------------- | HEADER |
 * tuple_rid | tuple_size | old_tuple_data | tuple_size | new_tuple_data |
 * ----------------------------------------------------------------------------------- For new page
 * type log record ------------------------------------ | HEADER | prev_page_id | page_id |
 * ------------------------------------
 */
public class LogRecord {
  private int size = 0;
  private int lsn = INVALID_LSN_ID;
  private int txn_id = Transaction.INVALID_TXN_ID;
  private int prev_lsn = INVALID_LSN_ID;
  private LogRecordType logRecordType = LogRecordType.INVALID;

  // For Delete Operation
  private RID delete_rid;
  private Tuple delete_tuple;

  // For Insert Operation
  private RID insert_rid;
  private Tuple insert_tuple;

  // For Update Operation
  private RID update_rid;
  private Tuple old_tuple;
  private Tuple new_tuple;

  // For New Page Operation
  private int prev_page_id;
  private int page_id;

  // constructor for Transaction type(BEGIN/COMMIT/ABORT)
  public LogRecord(int txn_id, int prev_lsn, LogRecordType logRecordType) {
    this.txn_id = txn_id;
    this.prev_lsn = prev_lsn;
    this.logRecordType = logRecordType;
    this.size = 20;
  }

  // constructor for INSERT/DELETE type
  public LogRecord(int txn_id, int prev_lsn, LogRecordType logRecordType, RID rid, Tuple tuple) {
    this.txn_id = txn_id;
    this.prev_lsn = prev_lsn;
    this.logRecordType = logRecordType;
    if (logRecordType == LogRecordType.INSERT) {
      insert_rid = rid;
      insert_tuple = tuple;
    } else {
      delete_rid = rid;
      delete_tuple = tuple;
    }
    // | HEADER=20 | tuple_rid=8 | tuple_size=4 | tuple_data=SIZE |
    this.size = 20 + 8 + 4 + tuple.getSize();
  }

  // constructor for UPDATE type
  public LogRecord(
      int txn_id,
      int prev_lsn,
      LogRecordType logRecordType,
      RID rid,
      Tuple old_tuple,
      Tuple new_tuple) {
    this.txn_id = txn_id;
    this.prev_lsn = prev_lsn;
    this.logRecordType = logRecordType;
    this.update_rid = rid;
    this.old_tuple = old_tuple;
    this.new_tuple = new_tuple;
    // | HEADER=20 | tuple_rid=8 | tuple_size=4 | old_tuple_data=SIZE | tuple_size=4 |
    // new_tuple_data=SIZE |
    this.size = 20 + 8 + 4 + old_tuple.getSize() + 4 + old_tuple.getSize();
  }

  // constructor for NEWPAGE type
  public LogRecord(
      int txn_id, int prev_lsn, LogRecordType logRecordType, int prev_page_id, int page_id) {
    this.txn_id = txn_id;
    this.prev_lsn = prev_lsn;
    this.logRecordType = logRecordType;
    this.prev_page_id = prev_page_id;
    this.page_id = page_id;
    // | HEADER | prev_page_id | page_id |
    this.size = 20 + 4 + 4;
  }

  public void setLsn(int lsn) {
    this.lsn = lsn;
  }

  public int getLsn() {
    return lsn;
  }

  public int getSize() {
    return size;
  }

  public int getTxn_id() {
    return txn_id;
  }

  public int getPage_id() {
    return page_id;
  }

  public int getPrev_lsn() {
    return prev_lsn;
  }

  public int getPrev_page_id() {
    return prev_page_id;
  }

  public RID getDelete_rid() {
    return delete_rid;
  }

  public RID getInsert_rid() {
    return insert_rid;
  }

  public RID getUpdate_rid() {
    return update_rid;
  }

  public Tuple getDelete_tuple() {
    return delete_tuple;
  }

  public Tuple getInsert_tuple() {
    return insert_tuple;
  }

  public Tuple getNew_tuple() {
    return new_tuple;
  }

  public Tuple getOld_tuple() {
    return old_tuple;
  }

  public LogRecordType getLogRecordType() {
    return logRecordType;
  }

  public static int INVALID_LSN_ID = -1;
}
