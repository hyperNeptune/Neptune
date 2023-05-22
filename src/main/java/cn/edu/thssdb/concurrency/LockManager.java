package cn.edu.thssdb.concurrency;

import cn.edu.thssdb.utils.RID;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

// As a talented locksmith in French history, Louis XVI endows our Lock Manager class.
public class LockManager {
    public enum LockMode {
        SHARED, EXCLUSIVE
    }

    public class LockRequest {
        public int txn_id;
        public LockMode lockMode;
        public String tableName;
        public Boolean granted = Boolean.FALSE;
        public RID rid;
        public LockRequest(int txn_id, LockMode lockMode, String tableName){
            this.txn_id = txn_id;
            this.lockMode = lockMode;
            this.tableName = tableName;
        }
        public LockRequest(int txn_id, LockMode lockMode, String tableName, RID rid){
            this.txn_id = txn_id;
            this.lockMode = lockMode;
            this.tableName = tableName;
            this.rid = rid;
        }
    }
    public class LockRequestQueue {
        public ArrayList<LockRequest> requestQueue;
        private final ReentrantLock latch = new ReentrantLock();
    }

    private HashMap<String, LockRequestQueue> tableLockMap = new HashMap<>();
    private final ReentrantLock tableLockMapLatch = new ReentrantLock();
    private final Condition tableLockLatchCondition = tableLockMapLatch.newCondition();

    private HashMap<RID, LockRequestQueue> rowLockMap = new HashMap<>();
    private final ReentrantLock rowLockMapLatch = new ReentrantLock();
    private final Condition rowLockLatchCondition = rowLockMapLatch.newCondition();


    private boolean CheckLock(Transaction txn, LockMode lockMode) {
        // check shrink state and abort state
        if (txn.getState() == TransactionState.SHRINKING || txn.getState() == TransactionState.ABORTED)
            return false;
        // 如果是 READ_UNCOMMITTED 可以直接回滚

        return true;
    }

    public boolean LockTable(Transaction txn, LockMode lockMode, String tableName) {
        if (!CheckLock(txn, lockMode))
            return false;
        if (txn.getIsolationLevel() == IsolationLevel.SERIALIZED){
            // 也许不需要这些

        } else if (txn.getIsolationLevel() == IsolationLevel.READ_COMMITTED) {
            // 检查是否已经有同级或更高级锁
            if (lockMode == LockMode.SHARED && (txn.IsTableSharedLocked(tableName) || txn.IsTableExclusiveLocked(tableName)))
                return true;
            // 检查对应的 tableLockMap 是否已经有锁
            tableLockMapLatch.lock();
            if(tableLockMap.get(tableName) == null) {
                LockRequestQueue newQueue = new LockRequestQueue();
                newQueue.latch.lock();
                newQueue.requestQueue.add(new LockRequest(txn.getTxn_id(), lockMode, tableName));
                newQueue.latch.unlock();
                tableLockMap.put(tableName, newQueue);
            }
            else {

            }
        }
        return true;
    }

    public void UnlockTable(Transaction txn, String table_name) {

    }

    public void LockRow(Transaction txn, LockMode lock_mode, String table_name, RID rid) {

    }

    public void UnlockRow(Transaction txn, String table_name, RID rid) {

    }

    private void UpgradeLockTable(Transaction txn, LockMode lock_mode, String table_name) {

    }

    private void UpgradeLockRow(Transaction txn, LockMode lock_mode, String table_name, RID rid) {

    }

    private Boolean AreLocksCompatible (LockMode l1, LockMode l2) {
        return l1 == LockMode.SHARED && l2 == LockMode.SHARED;
    }
}
