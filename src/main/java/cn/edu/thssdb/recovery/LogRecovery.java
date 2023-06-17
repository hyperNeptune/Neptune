package cn.edu.thssdb.recovery;

import cn.edu.thssdb.storage.DiskManager;
import cn.edu.thssdb.storage.Tuple;
import cn.edu.thssdb.utils.RID;

import java.nio.ByteBuffer;

/**
 * For every write operation on the table page, you should write ahead a corresponding log record.
 *
 * For EACH log record, HEADER is like (5 fields in common, 20 bytes in total).
 *---------------------------------------------
 * | size | LSN | transID | prevLSN | LogType |
 *---------------------------------------------
 * For insert type log record
 *---------------------------------------------------------------
 * | HEADER | tuple_rid | tuple_size | tuple_data(char[] array) |
 *---------------------------------------------------------------
 * For delete type (including markdelete, rollbackdelete, applydelete)
 *----------------------------------------------------------------
 * | HEADER | tuple_rid | tuple_size | tuple_data(char[] array) |
 *---------------------------------------------------------------
 * For update type log record
 *-----------------------------------------------------------------------------------
 * | HEADER | tuple_rid | tuple_size | old_tuple_data | tuple_size | new_tuple_data |
 *-----------------------------------------------------------------------------------
 * For new page type log record
 *--------------------------
 * | HEADER | prev_page_id |
 *--------------------------
 */

public class LogRecovery {
  public final DiskManager diskManager;

  public LogRecovery(DiskManager diskManager) {
    this.diskManager = diskManager;
  }

  public LogRecord DeserializeLogRecord(ByteBuffer buffer) {
    int size = buffer.getInt();
    int lsn = buffer.getInt();
    int txn_id = buffer.getInt();
    int prevLsn = buffer.getInt();
    int ordinalType = buffer.getInt();
    LogRecordType type = LogRecordType.values()[ordinalType];
    switch (type) {
      case INSERT:
      case APPLYDELETE:
        int rid_page = buffer.getInt();
        int rid_slot = buffer.getInt();
        int tuple_size = buffer.getInt();
        ByteBuffer tmpBuffer = ByteBuffer.allocate(tuple_size );
        for (int i = 0; i < tuple_size; i++) {
          byte b = buffer.get();
          tmpBuffer.put(b);
        }
        return new LogRecord(txn_id, prevLsn, type, new RID(rid_page, rid_slot), new Tuple(tmpBuffer));
      case BEGIN:
      case ABORT:
      case COMMIT:
        return new LogRecord(txn_id, prevLsn, type);
      case UPDATE:
        int u_rid_page = buffer.getInt();
        int u_rid_slot = buffer.getInt();
        int old_tuple_size = buffer.getInt();
        ByteBuffer old_tmpBuffer = ByteBuffer.allocate(old_tuple_size );
        for (int i = 0; i < old_tuple_size; i++) {
          byte b = buffer.get();
          old_tmpBuffer.put(b);
        }
        int new_tuple_size = buffer.getInt();
        ByteBuffer new_tmpBuffer = ByteBuffer.allocate(new_tuple_size );
        for (int i = 0; i < old_tuple_size; i++) {
          byte b = buffer.get();
          new_tmpBuffer.put(b);
        }
        return new LogRecord(txn_id, prevLsn, type, new RID(u_rid_page, u_rid_slot), new Tuple(old_tmpBuffer), new Tuple(new_tmpBuffer));
      default:
        return new LogRecord(-1, -1, LogRecordType.INVALID);
    }
  }
}
