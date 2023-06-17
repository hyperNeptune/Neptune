package cn.edu.thssdb.execution.executor;

import cn.edu.thssdb.concurrency.IsolationLevel;
import cn.edu.thssdb.concurrency.LockManager;
import cn.edu.thssdb.concurrency.Transaction;
import cn.edu.thssdb.execution.ExecContext;
import cn.edu.thssdb.schema.Schema;
import cn.edu.thssdb.schema.TableInfo;
import cn.edu.thssdb.storage.Tuple;
import cn.edu.thssdb.utils.Pair;
import cn.edu.thssdb.utils.RID;

import java.util.Iterator;

public class SeqScanExecutor extends Executor {
  Schema schema_;
  TableInfo tableInfo_;
  Tuple tuple_;
  RID rid_;
  Iterator<Pair<Tuple, RID>> iterator_;

  public SeqScanExecutor(TableInfo tableInfo, ExecContext ctx) {
    super(ctx);
    this.tableInfo_ = tableInfo;
    this.schema_ = tableInfo.getSchema();
  }

  @Override
  public void init() throws Exception {
    Transaction txn = getCtx().getTransaction();
    LockManager lockManager = getCtx().getLockManager();
    lockManager.lockTable(txn, LockManager.LockMode.INTENTION_SHARED, tableInfo_.getTableName());

    iterator_ = tableInfo_.getTable().iterator();
  }

  @Override
  public boolean next(Tuple tuple, RID rid) {
    Transaction txn = getCtx().getTransaction();
    LockManager lockManager = getCtx().getLockManager();

    if (iterator_.hasNext()) {
      Pair<Tuple, RID> pair = iterator_.next();
      tuple_ = pair.left;
      rid_ = pair.right;

      switch (txn.getIsolationLevel()) {
        case SERIALIZED:
          // 可串行化应当获取所有的锁，我这里设置成X锁，但还应该获取索引锁
          // TODO: 索引锁
          lockManager.lockRow(txn, LockManager.LockMode.EXCLUSIVE, tableInfo_.getTableName(), rid_);
          break;
        case REPEATABLE_READ:
        case READ_COMMITTED:
          lockManager.lockRow(txn, LockManager.LockMode.SHARED, tableInfo_.getTableName(), rid_);
          break;
        case READ_UNCOMMITTED:
          break;
      }

      tuple.copyAssign(tuple_);
      rid.assign(rid_);
      return true;
    } else {
      // 对于 RC 如果没有 next 可以提前释放获取过的锁
      if (txn.getIsolationLevel() == IsolationLevel.READ_COMMITTED) {
        txn.lockTxn();
        for (RID lock_rid : txn.getSharedRowLockSet().get(tableInfo_.getTableName())) {
          lockManager.unlockRow(txn, tableInfo_.getTableName(), lock_rid);
        }
        txn.unlockTxn();
      }
    }
    return false;
  }

  @Override
  public Schema getOutputSchema() {
    return schema_;
  }
}
