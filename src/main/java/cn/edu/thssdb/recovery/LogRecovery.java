package cn.edu.thssdb.recovery;

import cn.edu.thssdb.storage.DiskManager;

public class LogRecovery {
  public final DiskManager diskManager;

  public LogRecovery(DiskManager diskManager) {
    this.diskManager = diskManager;
  }

  public void DeserializeLogRecord() {}
}
