package neptune.concurrency;

enum AbortReason {
  LOCK_ON_SHRINKING,
  UPGRADE_CONFLICT,
  LOCK_SHARED_ON_READ_UNCOMMITTED,
  TABLE_LOCK_NOT_PRESENT,
  ATTEMPTED_INTENTION_LOCK_ON_ROW,
  TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS,
  INCOMPATIBLE_UPGRADE,
  ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD
};

public class TransactionAbortException extends Exception {
  private final int txn_id;
  private final AbortReason abortReason;

  public TransactionAbortException(int txn_id, AbortReason abortReason) {
    this.txn_id = txn_id;
    this.abortReason = abortReason;
  }

  public int getTxn_id() {
    return txn_id;
  }

  public AbortReason getAbortReason() {
    return abortReason;
  }

  public String getInfo() {
    switch (abortReason) {
      case UPGRADE_CONFLICT:
        return "Transaction "
            + txn_id
            + " aborted because another transaction is already waiting to upgrade its lock\n";
      case LOCK_ON_SHRINKING:
        return "Transaction "
            + txn_id
            + " aborted because it can not take locks in the shrinking state\n";
      case INCOMPATIBLE_UPGRADE:
        return "Transaction "
            + txn_id
            + " aborted because attempted lock upgrade is incompatible\n";
      case TABLE_LOCK_NOT_PRESENT:
        return "Transaction " + txn_id + " aborted because table lock not present\n";
      case ATTEMPTED_INTENTION_LOCK_ON_ROW:
        return "Transaction " + txn_id + " aborted because intention lock attempted on row\n";
      case LOCK_SHARED_ON_READ_UNCOMMITTED:
        return "Transaction " + txn_id + " aborted on lockshared on READ_UNCOMMITTED\n";
      case ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD:
        return "Transaction " + txn_id + " aborted because attempted to unlock but no lock held \n";
      case TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS:
        return "Transaction "
            + txn_id
            + " aborted because table locks dropped before dropping row locks\n";
    }
    return "";
  }
}
