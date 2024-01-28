package neptune.recovery;

import neptune.storage.DiskManager;
import neptune.storage.Tuple;
import neptune.utils.RID;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.ListIterator;

/**
 * For every write operation on the table page, you should write ahead a corresponding log record.
 *
 * <p>For EACH log record, HEADER is like (5 fields in common, 20 bytes in total).
 * --------------------------------------------- | size | LSN | transID | prevLSN | LogType |
 * --------------------------------------------- For insert type log record
 * --------------------------------------------------------------- | HEADER | tuple_rid | tuple_size
 * | tuple_data(char[] array) | --------------------------------------------------------------- For
 * delete type (including markdelete, rollbackdelete, applydelete)
 * ---------------------------------------------------------------- | HEADER | tuple_rid |
 * tuple_size | tuple_data(char[] array) |
 * --------------------------------------------------------------- For update type log record
 * ----------------------------------------------------------------------------------- | HEADER |
 * tuple_rid | tuple_size | old_tuple_data | tuple_size | new_tuple_data |
 * ----------------------------------------------------------------------------------- For new page
 * type log record -------------------------- | HEADER | prev_page_id | --------------------------
 */
public class LogRecovery {
  public final DiskManager diskManager;
  public final LogManager logManager;

  public LogRecovery(DiskManager diskManager, LogManager logManager) {
    this.diskManager = diskManager;
    this.logManager = logManager;
  }

  public void startRecovery() {
    ByteBuffer buffer = ByteBuffer.allocate(40000);
    try {
      int t = diskManager.readLog(buffer);
    } catch (Exception e) {
      e.printStackTrace();
    }
    ArrayList<LogRecord> logRecordArrayList = new ArrayList<>();
    buffer.flip();
    while (true) {
      LogRecord logRecord = DeserializeLogRecord(buffer);
      if (logRecord.getLogRecordType() != LogRecordType.INVALID) {
        logRecordArrayList.add(logRecord);
      } else {
        break;
      }
    }
    // Phase 1
    HashSet<Integer> undo_list = new HashSet<>();
    for (LogRecord logRecord : logRecordArrayList) {
      if (logRecord.getLogRecordType() == LogRecordType.BEGIN) {
        undo_list.add(logRecord.getTxn_id());
      } else if (logRecord.getLogRecordType() == LogRecordType.COMMIT
          || logRecord.getLogRecordType() == LogRecordType.ABORT) {
        undo_list.remove(logRecord.getTxn_id());
      } else {
        Redo(logRecord);
      }
    }
    // Phase 2
    ListIterator<LogRecord> iterator = logRecordArrayList.listIterator(logRecordArrayList.size());
    while (iterator.hasPrevious() && !undo_list.isEmpty()) {
      LogRecord logRecord = iterator.previous();
      if (logRecord.getLogRecordType() == LogRecordType.BEGIN) {
        undo_list.remove(logRecord.getTxn_id());
      } else {
        Undo(logRecord);
      }
    }
  }

  private void Redo(LogRecord logRecord) {
    if (logRecord.getLogRecordType() == LogRecordType.UPDATE) {
      diskManager.forceUpdate(logRecord.getUpdate_rid(), logRecord.getNew_tuple());
    }
  }

  private void Undo(LogRecord logRecord) {
    if (logRecord.getLogRecordType() == LogRecordType.UPDATE) {
      diskManager.forceUpdate(logRecord.getUpdate_rid(), logRecord.getOld_tuple());
    }
    // 新增一条
    logManager.appendLogRecord(
        new LogRecord(
            logRecord.getTxn_id(),
            logRecord.getPrev_lsn(),
            LogRecordType.ROLLBACKDELETE,
            logRecord.getUpdate_rid(),
            logRecord.getOld_tuple()));
  }

  public LogRecord DeserializeLogRecord(ByteBuffer buffer) {
    if (buffer.remaining() <= 0) return new LogRecord(-1, -1, LogRecordType.INVALID);
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
        ByteBuffer tmpBuffer = ByteBuffer.allocate(tuple_size);
        for (int i = 0; i < tuple_size; i++) {
          byte b = buffer.get();
          tmpBuffer.put(b);
        }
        return new LogRecord(
            txn_id, prevLsn, type, new RID(rid_page, rid_slot), new Tuple(tmpBuffer));
      case BEGIN:
      case ABORT:
      case COMMIT:
        return new LogRecord(txn_id, prevLsn, type);
      case UPDATE:
        int u_rid_page = buffer.getInt();
        int u_rid_slot = buffer.getInt();
        int old_tuple_size = buffer.getInt();
        ByteBuffer old_tmpBuffer = ByteBuffer.allocate(old_tuple_size);
        for (int i = 0; i < old_tuple_size; i++) {
          byte b = buffer.get();
          old_tmpBuffer.put(b);
        }
        int new_tuple_size = buffer.getInt();
        ByteBuffer new_tmpBuffer = ByteBuffer.allocate(new_tuple_size);
        for (int i = 0; i < old_tuple_size; i++) {
          byte b = buffer.get();
          new_tmpBuffer.put(b);
        }
        return new LogRecord(
            txn_id,
            prevLsn,
            type,
            new RID(u_rid_page, u_rid_slot),
            new Tuple(old_tmpBuffer),
            new Tuple(new_tmpBuffer));
      default:
        return new LogRecord(-1, -1, LogRecordType.INVALID);
    }
  }
}
