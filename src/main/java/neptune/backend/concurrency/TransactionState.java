package neptune.backend.concurrency;

enum TransactionState {
  GROWING,
  SHRINKING,
  COMMITTED,
  ABORTED
}
