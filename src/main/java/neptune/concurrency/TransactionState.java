package neptune.concurrency;

enum TransactionState {
  GROWING,
  SHRINKING,
  COMMITTED,
  ABORTED
}
