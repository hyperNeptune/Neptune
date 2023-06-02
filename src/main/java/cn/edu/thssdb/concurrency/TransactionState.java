package cn.edu.thssdb.concurrency;

enum TransactionState {
  GROWING,
  SHRINKING,
  COMMITTED,
  ABORTED
}
