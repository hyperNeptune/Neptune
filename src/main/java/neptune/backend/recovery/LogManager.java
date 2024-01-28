package neptune.backend.recovery;

import neptune.backend.storage.DiskManager;
import neptune.backend.storage.Tuple;

import java.nio.ByteBuffer;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
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
  private final ReentrantLock buffer_latch = new ReentrantLock();

  private void putLogHeader(LogRecord logRecord) {
    logBuffer.putInt(logRecord.getSize());
    logBuffer.putInt(logRecord.getLsn());
    logBuffer.putInt(logRecord.getTxn_id());
    logBuffer.putInt(logRecord.getPrev_lsn());
    logBuffer.putInt(logRecord.getLogRecordType().ordinal());
  }

  public int appendLogRecord(LogRecord logRecord) {
    buffer_latch.lock();
    // 20 HEADER
    // | size | LSN | transID | prevLSN | LogType |
    int lsn = assignLsn();
    logRecord.setLsn(lsn);

    // INSERT
    // RID: PageID, SlotID
    if (logRecord.getLogRecordType() == LogRecordType.INSERT) {
      Tuple l_tuple = logRecord.getInsert_tuple();
      int size = 20 + 8 + 4 + l_tuple.getSize();
      if (logBuffer.remaining() <= size) {
        flushLogs();
      }
      putLogHeader(logRecord);
      logBuffer.putInt(logRecord.getInsert_rid().getPageId());
      logBuffer.putInt(logRecord.getInsert_rid().getSlotId());
      logBuffer.putInt(l_tuple.getSize());
      ByteBuffer tmpBuffer = ByteBuffer.allocate(l_tuple.getSize());
      l_tuple.serialize(tmpBuffer, 0);
      logBuffer.put(tmpBuffer);
    }

    // DELETE
    else if (logRecord.getLogRecordType() == LogRecordType.APPLYDELETE
        || logRecord.getLogRecordType() == LogRecordType.MARKDELETE
        || logRecord.getLogRecordType() == LogRecordType.ROLLBACKDELETE) {
      Tuple l_tuple = logRecord.getDelete_tuple();
      int size = 20 + 8 + 4 + l_tuple.getSize();
      if (logBuffer.remaining() <= size) {
        flushLogs();
      }
      putLogHeader(logRecord);
      logBuffer.putInt(logRecord.getDelete_rid().getPageId());
      logBuffer.putInt(logRecord.getDelete_rid().getSlotId());
      logBuffer.putInt(l_tuple.getSize());
      ByteBuffer tmpBuffer = ByteBuffer.allocate(l_tuple.getSize());
      l_tuple.serialize(tmpBuffer, 0);
      logBuffer.put(tmpBuffer);
    }

    // UPDATE
    else if (logRecord.getLogRecordType() == LogRecordType.UPDATE) {
      // | HEADER | tuple_rid | tuple_size | old_tuple_data | tuple_size | new_tuple_data |
      Tuple o_tuple = logRecord.getOld_tuple();
      Tuple n_tuple = logRecord.getNew_tuple();
      int size = 20 + 8 + 4 + o_tuple.getSize() + 4 + n_tuple.getSize();
      if (logBuffer.remaining() <= size) {
        flushLogs();
      }
      putLogHeader(logRecord);

      logBuffer.putInt(logRecord.getUpdate_rid().getPageId());
      logBuffer.putInt(logRecord.getUpdate_rid().getSlotId());

      ByteBuffer tmpBuffer1 = ByteBuffer.allocate(o_tuple.getSize());
      ByteBuffer tmpBuffer2 = ByteBuffer.allocate(n_tuple.getSize());
      o_tuple.serialize(tmpBuffer1, 0);
      n_tuple.serialize(tmpBuffer2, 0);
      logBuffer.putInt(o_tuple.getSize());
      logBuffer.put(tmpBuffer1);
      logBuffer.putInt(n_tuple.getSize());
      logBuffer.put(tmpBuffer2);
    }

    // NEW PAGE
    else if (logRecord.getLogRecordType() == LogRecordType.NEWPAGE) {
      int size = 20 + 8;
      if (logBuffer.remaining() <= size) {
        flushLogs();
      }
      putLogHeader(logRecord);
      logBuffer.putInt(logRecord.getPrev_page_id());
      logBuffer.putInt(logRecord.getPage_id());
    } else {
      if (logBuffer.remaining() <= 20) {
        flushLogs();
      }
      putLogHeader(logRecord);
    }

    buffer_latch.unlock();

    return lsn;
  }

  public LogManager(DiskManager diskManager) {
    currentLsn = 0;
    this.diskManager = diskManager;
    ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
    executor.scheduleAtFixedRate(this::flushLogs, 1, 5, TimeUnit.SECONDS);
  }

  public void forceFlushLogs() {
    flushLogs();
  }

  private void flushLogs() {
    try {
      buffer_latch.lock();
      diskManager.writeLog(logBuffer);
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      buffer_latch.unlock();
    }
  }

  private int assignLsn() {
    lsn_latch.lock();
    currentLsn++;
    int result = currentLsn;
    lsn_latch.unlock();
    return result;
  }
}
