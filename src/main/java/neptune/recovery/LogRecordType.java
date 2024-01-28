package neptune.recovery;

public enum LogRecordType {
  INVALID,
  INSERT,
  MARKDELETE,
  APPLYDELETE,
  ROLLBACKDELETE,
  UPDATE,
  BEGIN,
  COMMIT,
  ABORT,
  NEWPAGE,
}
