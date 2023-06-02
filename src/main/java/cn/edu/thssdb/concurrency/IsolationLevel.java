package cn.edu.thssdb.concurrency;

public enum IsolationLevel {
  READ_COMMITTED,
  SERIALIZED,
  READ_UNCOMMITTED,
  REPEATABLE_READ
}
