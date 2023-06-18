package cn.edu.thssdb.concurrency;

import cn.edu.thssdb.utils.RID;

import java.util.*;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

// As a talented locksmith in French history, Louis XVI endows our Lock Manager class.
public class LockManager {
  public enum LockMode {
    SHARED,
    EXCLUSIVE,
    INTENTION_SHARED,
    INTENTION_EXCLUSIVE,
    SHARED_INTENTION_EXCLUSIVE
  }

  public class LockRequest {
    public int txn_id;
    public LockMode lockMode;
    public String tableName;
    public long threadid;
    public Boolean granted = Boolean.FALSE;
    public RID rid = new RID();

    public LockRequest(int txn_id, LockMode lockMode, String tableName, long threadid) {
      this.txn_id = txn_id;
      this.lockMode = lockMode;
      this.tableName = tableName;
      this.threadid = threadid;
    }

    public LockRequest(int txn_id, LockMode lockMode, String tableName, RID rid, long threadid) {
      this.txn_id = txn_id;
      this.lockMode = lockMode;
      this.tableName = tableName;
      this.rid = rid;
      this.threadid = threadid;
    }
  }

  public class LockRequestQueue {
    public ArrayList<LockRequest> requestQueue = new ArrayList<>();
    public final ReentrantLock latch = new ReentrantLock();
    public final Condition latchCondition = latch.newCondition();
    public int upgrading = Transaction.INVALID_TXN_ID;
  }

  private final HashMap<String, LockRequestQueue> tableLockMap = new HashMap<>();
  private final ReentrantLock tableLockMapLatch = new ReentrantLock();
  private final Condition tableLockLatchCondition = tableLockMapLatch.newCondition();

  private final HashMap<RID, LockRequestQueue> rowLockMap = new HashMap<>();
  private final ReentrantLock rowLockMapLatch = new ReentrantLock();
  private final Condition rowLockLatchCondition = rowLockMapLatch.newCondition();

  /*
  REPEATABLE_READ:
     The transaction is required to take all locks.
     All locks are allowed in the GROWING state
     No locks are allowed in the SHRINKING state

  READ_COMMITTED:
     The transaction is required to take all locks.
     All locks are allowed in the GROWING state
     Only IS, S locks are allowed in the SHRINKING state

  READ_UNCOMMITTED:
     The transaction is required to take only IX, X locks.
     X, IX locks are allowed in the GROWING state.
     S, IS, SIX locks are never allowed
  */

  public boolean lockTable(Transaction txn, LockMode lockMode, String tableName) {
    // 暂时没有考虑 SERIALIZABLE

    // ABORT 或者 COMMITTED 直接干掉
    if (txn.getState() == TransactionState.ABORTED || txn.getState() == TransactionState.COMMITTED)
      return false;

    // SHRINKING
    if (txn.getState() == TransactionState.SHRINKING) {
      if (txn.getIsolationLevel() == IsolationLevel.REPEATABLE_READ)
        // LOCK_ON_SHRINKING
        return false;
      if (txn.getIsolationLevel() == IsolationLevel.READ_COMMITTED
          && (lockMode != LockMode.SHARED && lockMode != LockMode.INTENTION_SHARED))
        // LOCK_ON_SHRINKING
        return false;
    }

    // 另外考虑 READ_UNCOMMITTED 无法获得 S
    if (txn.getIsolationLevel() == IsolationLevel.READ_UNCOMMITTED
        && (lockMode == LockMode.SHARED
            || lockMode == LockMode.INTENTION_SHARED
            || lockMode == LockMode.SHARED_INTENTION_EXCLUSIVE))
      // LOCK_SHARED_ON_READ_UNCOMMITTED
      return false;

    // 检查是否已经有同级或更高级锁
    // TODO
    if (lockMode == LockMode.SHARED
        && (txn.IsTableSharedLocked(tableName) || txn.IsTableExclusiveLocked(tableName)))
      return true;

    // 检查对应的 tableLockMap 是否已经有其他的锁
    LockRequestQueue currentQueue;
    LockRequest currentRequest =
        new LockRequest(txn.getTxn_id(), lockMode, tableName, txn.getThread_id());
    tableLockMapLatch.lock();
    if (tableLockMap.get(tableName) == null) {
      // 没有，新建一个 RequestQueue
      LockRequestQueue newQueue = new LockRequestQueue();
      newQueue.latch.lock();
      newQueue.requestQueue.add(currentRequest);
      currentQueue = newQueue;
      newQueue.latch.unlock();
      tableLockMap.put(tableName, newQueue);
    } else {
      // 有，在已有的 RequestQueue 上添加
      LockRequestQueue queue = tableLockMap.get(tableName);
      currentQueue = queue;

      queue.latch.lock();
      Optional<LockRequest> oLockRequest =
          queue.requestQueue.stream()
              .filter(lockRequest -> lockRequest.tableName.equals(tableName))
              .findFirst();
      queue.latch.unlock();

      // 是一次锁升级
      if (oLockRequest.isPresent()
          && oLockRequest.get().lockMode == LockMode.SHARED
          && lockMode == LockMode.EXCLUSIVE
          && oLockRequest.get().txn_id == txn.getTxn_id()) {
        // 检查是否可以升级：升级条件 没有其他事务在同一资源上升级
        if (queue.upgrading != Transaction.INVALID_TXN_ID) {
          // UPGRADE_CONFLICT
          return false;
        }

        // 释放当前已经持有的锁，并在 queue 中标记正在尝试升级。
        queue.latch.lock();
        unlockTable(txn, tableName); // TODO: 这里改过，可能爆炸
        queue.upgrading = txn.getTxn_id();
        queue.latch.unlock();

        // 等待直到新锁被授予。
        queue.latch.lock();
        queue.requestQueue.add(currentRequest);
        queue.upgrading = Transaction.INVALID_TXN_ID;
        queue.latch.unlock();
      } else {
        queue.latch.lock();
        queue.requestQueue.add(currentRequest);
        queue.latch.unlock();
      }
    }
    tableLockMapLatch.unlock();

    // 获取锁
    currentQueue.latch.lock();
    try {

      while (!grantLock(currentQueue, currentRequest)) {
        try {
          currentQueue.latchCondition.await();
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    } finally {
      currentQueue.latch.unlock();
      // txn book-keeping
      addTxnTableLockSet(txn, lockMode, tableName);
    }

    return true;
  }

  private boolean grantLock(LockRequestQueue lockRequestQueue, LockRequest currentRequest) {
    // System.out.println(t + ": try grant lock");
    // 判断兼容性。遍历请求队列，查看当前锁请求是否与所有的已经 granted 的请求兼容。
    for (LockRequest request : lockRequestQueue.requestQueue) {
      if (request != currentRequest && request.granted) {
        if (!areLocksCompatible(request.lockMode, currentRequest.lockMode)
            && request.txn_id != currentRequest.txn_id) return false;
        if (currentRequest.lockMode == LockMode.SHARED
            && request.lockMode == LockMode.EXCLUSIVE
            && request.txn_id == currentRequest.txn_id) return false;
      }
    }
    // 特殊处理一种常见情况
    if (currentRequest.lockMode == LockMode.EXCLUSIVE) {
      ArrayList<LockRequest> grantedList = new ArrayList<>();
      for (LockRequest request : lockRequestQueue.requestQueue) {
        if (request.granted) grantedList.add(request);
      }
      if (grantedList.size() == 1
          && grantedList.get(0).txn_id == currentRequest.txn_id
          && grantedList.get(0).lockMode == LockMode.SHARED) {
        lockRequestQueue.requestQueue.remove(grantedList.get(0));
        currentRequest.granted = true;
        return true;
      }
    }

    // 判断优先级。锁请求会以严格的 FIFO 顺序依次满足。
    if (lockRequestQueue.upgrading != Transaction.INVALID_TXN_ID) {
      // 当前队列中存在锁升级请求，如果为自己，则优先级最高
      if (lockRequestQueue.upgrading == currentRequest.txn_id) {
        currentRequest.granted = true;
        return true;
      } else {
        return false;
      }
    } else {
      // 若队列中不存在锁升级请求，则遍历队列。如果当前请求是第一个 waiting 状态的请求，则代表优先级最高。
      ArrayList<LockRequest> waitingList =
          lockRequestQueue.requestQueue.stream()
              .filter(lockRequest -> lockRequest.granted.equals(false))
              .collect(Collectors.toCollection(ArrayList::new));
      if (waitingList.isEmpty()) {
        currentRequest.granted = true;
        return true;
      }

      //  如果当前请求前面还存在其他 waiting 请求，则要判断当前请求是否前面的 waiting 请求兼容。
      //  若兼容，则仍可以视为优先级最高。若存在不兼容的请求，则优先级不为最高。
      for (LockRequest lr : waitingList) {
        if (lr.equals(currentRequest)) {
          currentRequest.granted = true;
          return true;
        }
        if (!areLocksCompatible(lr.lockMode, currentRequest.lockMode)) return false;
      }
    }

    return false;
  }

  //  REPEATABLE_READ:
  //      Unlocking S/X locks should set the transaction state to SHRINKING
  //
  //  READ_COMMITTED:
  //      Unlocking X locks should set the transaction state to SHRINKING.
  //      Unlocking S locks does not affect transaction state.
  //
  // READ_UNCOMMITTED:
  //      Unlocking X locks should set the transaction state to SHRINKING.
  //      S locks are not permitted under READ_UNCOMMITTED.
  //          The behaviour upon unlocking an S lock under this isolation level is undefined.

  public boolean unlockTable(Transaction txn, String tableName) {
    // 首先检查其下所有 rowlock 是否已经释放
    // TODO: 先不检查了, 正常不会这样
    // 获取对应的 lock request queue
    tableLockMapLatch.lock();
    LockRequestQueue lockRequestQueue = tableLockMap.get(tableName);
    tableLockMapLatch.unlock();
    lockRequestQueue.latch.lock();
    if (lockRequestQueue.requestQueue.stream()
        .noneMatch(lockRequest -> lockRequest.txn_id == txn.getTxn_id())) {
      lockRequestQueue.latch.unlock();
      System.out.println(txn.getTxn_id() + "????????????");
      // ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD
      return false;
    }
    // 遍历请求队列，找到 unlock 对应的 granted 请求
    Optional<LockRequest> oRequest =
        lockRequestQueue.requestQueue.stream()
            .filter(
                lockRequest ->
                    lockRequest.tableName.equals(tableName)
                        && lockRequest.txn_id == txn.getTxn_id())
            .findFirst();
    if (oRequest.isPresent()) {
      LockRequest request = oRequest.get();
      if (txn.getIsolationLevel() == IsolationLevel.REPEATABLE_READ) {
        // 当隔离级别为 REPEATABLE_READ 时，S/X 锁释放会使事务进入 Shrinking 状态。
        if (request.lockMode == LockMode.SHARED || request.lockMode == LockMode.EXCLUSIVE) {
          txn.lockTxn();
          txn.setState(TransactionState.SHRINKING);
          txn.unlockTxn();
        }
      }
      if (txn.getIsolationLevel() == IsolationLevel.READ_COMMITTED) {
        // 当为 READ_COMMITTED 时，只有 X 锁释放使事务进入 Shrinking 状态。
        if (request.lockMode == LockMode.EXCLUSIVE) {
          txn.lockTxn();
          txn.setState(TransactionState.SHRINKING);
          txn.unlockTxn();
        }
      }
      if (txn.getIsolationLevel() == IsolationLevel.READ_UNCOMMITTED) {
        // 当为 READ_UNCOMMITTED 时，X 锁释放使事务 Shrinking，S 锁不会出现。
        if (request.lockMode == LockMode.EXCLUSIVE) {
          txn.lockTxn();
          txn.setState(TransactionState.SHRINKING);
          txn.unlockTxn();
        }
      }
      request.granted = false;
      lockRequestQueue.requestQueue.remove(request);
    }
    lockRequestQueue.latchCondition.signalAll();
    lockRequestQueue.latch.unlock();

    // txn book keeping
    removeTxnTableLockSet(txn, tableName);
    return true;
  }

  public boolean lockRow(Transaction txn, LockMode lock_mode, String table_name, RID rid) {
    RID justiceRid = new RID(rid.getPageId(), rid.getSlotId());

    // 必须先持有 table lock 再持有 row lock
    // TODO: 先不检查了

    // ABORT 或者 COMMITTED 直接干掉
    if (txn.getState() == TransactionState.ABORTED || txn.getState() == TransactionState.COMMITTED)
      return false;

    // SHRINKING
    if (txn.getState() == TransactionState.SHRINKING) {
      if (txn.getIsolationLevel() == IsolationLevel.REPEATABLE_READ)
        // LOCK_ON_SHRINKING
        return false;
      if (txn.getIsolationLevel() == IsolationLevel.READ_COMMITTED
          && (lock_mode != LockMode.SHARED && lock_mode != LockMode.INTENTION_SHARED))
        // LOCK_ON_SHRINKING
        return false;
    }

    // 另外考虑 READ_UNCOMMITTED 无法获得 S
    if (txn.getIsolationLevel() == IsolationLevel.READ_UNCOMMITTED
        && (lock_mode == LockMode.SHARED
            || lock_mode == LockMode.INTENTION_SHARED
            || lock_mode == LockMode.SHARED_INTENTION_EXCLUSIVE))
      // LOCK_SHARED_ON_READ_UNCOMMITTED
      return false;

    // 检查是否已经有同级或更高级锁
    if (lock_mode == LockMode.SHARED
        && (txn.IsRowSharedLocked(table_name, justiceRid)
            || txn.IsRowExclusiveLocked(table_name, justiceRid))) return true;
    if (lock_mode == LockMode.EXCLUSIVE && txn.IsRowExclusiveLocked(table_name, justiceRid))
      return true;

    // 检查对应的 rowLockMap 是否已经有其他的锁
    LockRequestQueue currentQueue;
    LockRequest currentRequest =
        new LockRequest(txn.getTxn_id(), lock_mode, table_name, justiceRid, txn.getThread_id());
    rowLockMapLatch.lock();
    if (rowLockMap.get(justiceRid) == null) {
      // 没有，新建一个 RequestQueue
      LockRequestQueue newQueue = new LockRequestQueue();
      newQueue.latch.lock();
      newQueue.requestQueue.add(currentRequest);
      currentQueue = newQueue;
      newQueue.latch.unlock();
      rowLockMap.put(justiceRid, newQueue);
    } else {
      // 有，在已有的 RequestQueue 上添加
      LockRequestQueue queue = rowLockMap.get(justiceRid);
      currentQueue = queue;

      queue.latch.lock();
      Optional<LockRequest> oLockRequest =
          queue.requestQueue.stream()
              .filter(
                  lockRequest ->
                      lockRequest.rid.equals(justiceRid)
                          && lockRequest.tableName.equals(table_name))
              .findFirst();
      queue.latch.unlock();

      // 是一次锁升级
      if (oLockRequest.isPresent()
          && oLockRequest.get().lockMode == LockMode.SHARED
          && lock_mode == LockMode.EXCLUSIVE
          && oLockRequest.get().txn_id == txn.getTxn_id()) {
        // 检查是否可以升级：升级条件 没有其他事务在同一资源上升级
        if (queue.upgrading != Transaction.INVALID_TXN_ID) {
          // UPGRADE_CONFLICT
          return false;
        }
        // System.out.println("[RowLock Upgrade] " + oLockRequest.get().rid + ", Thread:" +
        // oLockRequest.get().threadid + ", Txn:" +oLockRequest.get().txn_id);

        // 释放当前已经持有的锁，并在 queue 中标记正在尝试升级。
        queue.latch.lock();
        unlockRow(txn, table_name, justiceRid);
        queue.upgrading = txn.getTxn_id();
        queue.latch.unlock();

        // 等待直到新锁被授予。
        queue.latch.lock();
        queue.requestQueue.add(currentRequest);
        queue.upgrading = Transaction.INVALID_TXN_ID;
        queue.latch.unlock();

        //        // 我他妈直接更改锁的等级
        //        queue.latch.lock();
        //        queue.latch.unlock();
      } else {
        queue.latch.lock();
        queue.requestQueue.add(currentRequest);
        queue.latch.unlock();
      }
    }
    rowLockMapLatch.unlock();

    // 获取锁
    currentQueue.latch.lock();
    try {
      while (!grantLock(currentQueue, currentRequest)) {
        try {
          //                    System.out.println("[RowLock Waiting]" + "Txn: " + txn.getTxn_id() +
          // ", Thread:" + txn.getThread_id() +
          //                            ", waiting for " + currentQueue.requestQueue.get(0).rid);
          //                    System.out.println("[RowLock Waiting Spec] Current Waiting Queue:");
          //                    for (LockRequest lr : currentQueue.requestQueue)
          //                    {
          //                      System.out.println("\t" + lr.lockMode + ", Txn:" + lr.txn_id + ",
          // Granted:"
          //           + lr.granted + ", Thread:" + lr.threadid);
          //                    }
          currentQueue.latchCondition.await();
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    } finally {
      currentQueue.latch.unlock();
      addTxnRowLockSet(txn, lock_mode, justiceRid, table_name);
    }
    return true;
  }

  public boolean unlockRow(Transaction txn, String table_name, RID rid) {
    RID justiceRid = new RID(rid.getPageId(), rid.getSlotId());
    // 获取对应的 lock request queue
    // System.out.println("[UnlockRow] Txn:" + txn.getTxn_id() + ", Thread:" + txn.getThread_id() +
    // ", " + rid);
    rowLockMapLatch.lock();
    LockRequestQueue lockRequestQueue = rowLockMap.get(justiceRid);
    if (lockRequestQueue == null) {
      System.out.println("???");
      return false;
    }
    // RID temp = lockRequestQueue.requestQueue.get(0).rid;
    rowLockMapLatch.unlock();

    lockRequestQueue.latch.lock();
    if (lockRequestQueue.requestQueue.stream()
        .noneMatch(lockRequest -> lockRequest.txn_id == txn.getTxn_id())) {
      lockRequestQueue.latch.unlock();
      System.out.println(txn.getTxn_id() + "????????????" + justiceRid.toString());
      // ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD
      return false;
    }
    // 遍历请求队列，找到 unlock 对应的 granted 请求
    Optional<LockRequest> oRequest =
        lockRequestQueue.requestQueue.stream()
            .filter(
                lockRequest ->
                    lockRequest.tableName.equals(table_name)
                        && lockRequest.rid.equals(justiceRid)
                        && lockRequest.txn_id == txn.getTxn_id())
            .findFirst();
    if (oRequest.isPresent()) {
      LockRequest request = oRequest.get();
      if (txn.getIsolationLevel() == IsolationLevel.REPEATABLE_READ) {
        // 当隔离级别为 REPEATABLE_READ 时，S/X 锁释放会使事务进入 Shrinking 状态。
        if (request.lockMode == LockMode.SHARED || request.lockMode == LockMode.EXCLUSIVE) {
          txn.lockTxn();
          txn.setState(TransactionState.SHRINKING);
          txn.unlockTxn();
        }
      }
      if (txn.getIsolationLevel() == IsolationLevel.READ_COMMITTED) {
        // 当为 READ_COMMITTED 时，只有 X 锁释放使事务进入 Shrinking 状态。
        if (request.lockMode == LockMode.EXCLUSIVE) {
          txn.lockTxn();
          txn.setState(TransactionState.SHRINKING);
          txn.unlockTxn();
        }
      }
      if (txn.getIsolationLevel() == IsolationLevel.READ_UNCOMMITTED) {
        // 当为 READ_UNCOMMITTED 时，X 锁释放使事务 Shrinking，S 锁不会出现。
        if (request.lockMode == LockMode.EXCLUSIVE) {
          txn.lockTxn();
          txn.setState(TransactionState.SHRINKING);
          txn.unlockTxn();
        }
      }
      request.granted = false;
      lockRequestQueue.requestQueue.remove(request);
      //      if (lockRequestQueue.requestQueue.isEmpty()) {
      //        rowLockMapLatch.lock();
      //        rowLockMap.remove(justiceRid);
      //        rowLockMapLatch.unlock();
      //      }
    }
    // System.out.println("[RowLock Unlock]" + "Txn: " + txn.getTxn_id() + ", Thread:" +
    // txn.getThread_id() + ", " + temp);
    lockRequestQueue.latchCondition.signalAll();
    lockRequestQueue.latch.unlock();

    // txn book keeping
    removeTxnRowLockSet(txn, justiceRid, table_name);

    //    if (lockRequestQueue.requestQueue.isEmpty()) {
    //      rowLockMapLatch.lock();
    //      rowLockMap.remove(justiceRid);
    //      rowLockMapLatch.unlock();
    //    }

    return true;
  }

  private void addTxnTableLockSet(Transaction txn, LockMode lockMode, String tableName) {
    txn.lockTxn();
    if (lockMode == LockMode.SHARED || lockMode == LockMode.INTENTION_SHARED) {
      txn.getSharedTableLockSet().add(tableName);
    }
    if (lockMode == LockMode.EXCLUSIVE || lockMode == LockMode.INTENTION_EXCLUSIVE) {
      txn.getExclusiveTableLockSet().add(tableName);
    }
    txn.unlockTxn();
  }

  private void removeTxnTableLockSet(Transaction txn, String tableName) {
    txn.lockTxn();
    txn.getSharedTableLockSet().remove(tableName);
    txn.getExclusiveTableLockSet().remove(tableName);
    txn.unlockTxn();
  }

  private void addTxnRowLockSet(Transaction txn, LockMode lockMode, RID rid, String tableName) {
    txn.lockTxn();
    if (lockMode == LockMode.SHARED || lockMode == LockMode.INTENTION_SHARED) {
      if (txn.getSharedRowLockSet().get(tableName) == null) {
        HashSet<RID> set = new HashSet<>();
        set.add(rid);
        txn.getSharedRowLockSet().put(tableName, set);
      } else {
        HashSet<RID> set = txn.getSharedRowLockSet().get(tableName);
        set.add(rid);
      }
    }
    if (lockMode == LockMode.EXCLUSIVE || lockMode == LockMode.INTENTION_EXCLUSIVE) {
      if (txn.getExclusiveRowLockSet().get(tableName) == null) {
        HashSet<RID> set = new HashSet<>();
        set.add(rid);
        txn.getExclusiveRowLockSet().put(tableName, set);
      } else {
        HashSet<RID> set = txn.getExclusiveRowLockSet().get(tableName);
        set.add(rid);
      }
    }
    txn.unlockTxn();
  }

  private void removeTxnRowLockSet(Transaction txn, RID rid, String tableName) {
    txn.lockTxn();
    if (txn.getSharedRowLockSet().get(tableName) != null)
      txn.getSharedRowLockSet().get(tableName).remove(rid);
    if (txn.getExclusiveRowLockSet().get(tableName) != null)
      txn.getExclusiveRowLockSet().get(tableName).remove(rid);
    txn.unlockTxn();
  }

  private Boolean areLocksCompatible(LockMode l1, LockMode l2) {
    // return l1 == LockMode.SHARED && l2 == LockMode.SHARED;
    if (l1 == LockMode.EXCLUSIVE || l2 == LockMode.EXCLUSIVE) return false;
    if ((l1 == LockMode.SHARED_INTENTION_EXCLUSIVE && l2 != LockMode.INTENTION_SHARED)
        || (l2 == LockMode.SHARED_INTENTION_EXCLUSIVE && l1 != LockMode.INTENTION_SHARED))
      return false;
    return (l1 != LockMode.SHARED || l2 != LockMode.INTENTION_EXCLUSIVE)
        && (l2 != LockMode.SHARED || l1 != LockMode.INTENTION_EXCLUSIVE);
  }
}
