package cn.edu.thssdb.recovery;

import cn.edu.thssdb.storage.DiskManager;
import cn.edu.thssdb.storage.Tuple;

import java.nio.ByteBuffer;
import java.util.concurrent.locks.ReentrantLock;

// French historian
// Aries
public class LogManager {
  private final ByteBuffer logBuffer = ByteBuffer.allocate(45056);
  private final DiskManager diskManager;

  public void runFlushThread() {}

  public void stopFlushThread() {}

  private int currentLsn;
  private final ReentrantLock lsn_latch = new ReentrantLock();

  public int appendLogRecord(LogRecord logRecord) {
    // 20 HEADER
    // | size | LSN | transID | prevLSN | LogType |
    int lsn = assignLsn();
    logRecord.setLsn(lsn);
    logBuffer.putInt(logRecord.getSize());
    logBuffer.putInt(lsn);
    logBuffer.putInt(logRecord.getTxn_id());
    logBuffer.putInt(logRecord.getPrev_lsn());
    logBuffer.putInt(logRecord.getLogRecordType().ordinal());

    // INSERT
    // RID: PageID, SlotID
    if (logRecord.getLogRecordType() == LogRecordType.INSERT) {
      logBuffer.putInt(logRecord.getInsert_rid().getPageId());
      logBuffer.putInt(logRecord.getInsert_rid().getSlotId());
      Tuple l_tuple = logRecord.getInsert_tuple();
      logBuffer.putInt(l_tuple.getSize());
      ByteBuffer tmpBuffer = ByteBuffer.allocate(4096);
      l_tuple.serialize(tmpBuffer, 0);
      logBuffer.put(tmpBuffer);
    }

    // DELETE
    if (logRecord.getLogRecordType() == LogRecordType.APPLYDELETE
        || logRecord.getLogRecordType() == LogRecordType.MARKDELETE
        || logRecord.getLogRecordType() == LogRecordType.ROLLBACKDELETE) {
      logBuffer.putInt(logRecord.getDelete_rid().getPageId());
      logBuffer.putInt(logRecord.getDelete_rid().getSlotId());
      Tuple l_tuple = logRecord.getDelete_tuple();
      logBuffer.putInt(l_tuple.getSize());
      ByteBuffer tmpBuffer = ByteBuffer.allocate(4096);
      l_tuple.serialize(tmpBuffer, 0);
      logBuffer.put(tmpBuffer);
    }

    // UPDATE
    if (logRecord.getLogRecordType() == LogRecordType.UPDATE) {
      // | HEADER | tuple_rid | tuple_size | old_tuple_data | tuple_size | new_tuple_data |
      logBuffer.putInt(logRecord.getUpdate_rid().getPageId());
      logBuffer.putInt(logRecord.getUpdate_rid().getSlotId());
      Tuple o_tuple = logRecord.getOld_tuple();
      Tuple n_tuple = logRecord.getNew_tuple();
      ByteBuffer tmpBuffer1 = ByteBuffer.allocate(4096);
      ByteBuffer tmpBuffer2 = ByteBuffer.allocate(4096);
      o_tuple.serialize(tmpBuffer1, 0);
      n_tuple.serialize(tmpBuffer2, 0);
      logBuffer.putInt(o_tuple.getSize());
      logBuffer.put(tmpBuffer1);
      logBuffer.putInt(n_tuple.getSize());
      logBuffer.put(tmpBuffer2);
    }

    // NEW PAGE
    if (logRecord.getLogRecordType() == LogRecordType.NEWPAGE) {
      logBuffer.putInt(logRecord.getPrev_page_id());
      logBuffer.putInt(logRecord.getPage_id());
    }

    return lsn;
  }

  public LogManager(DiskManager diskManager) {
    currentLsn = 0;
    this.diskManager = diskManager;
  }

  private int assignLsn() {
    lsn_latch.lock();
    currentLsn++;
    int result = currentLsn;
    lsn_latch.unlock();
    return result;
  }
}
