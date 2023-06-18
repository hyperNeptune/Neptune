package cn.edu.thssdb.execution.executor;

import cn.edu.thssdb.concurrency.LockManager;
import cn.edu.thssdb.concurrency.Transaction;
import cn.edu.thssdb.concurrency.TransactionManager;
import cn.edu.thssdb.execution.ExecContext;
import cn.edu.thssdb.schema.Schema;
import cn.edu.thssdb.schema.TableInfo;
import cn.edu.thssdb.storage.Tuple;
import cn.edu.thssdb.type.Value;
import cn.edu.thssdb.utils.RID;

public class indexDeleteExecutor extends Executor {
  private final Value<?, ?> delValue_;
  private final TableInfo tableInfo_;

  public indexDeleteExecutor(ExecContext ctx, Value<?, ?> delValue, TableInfo tableInfo) {
    super(ctx);
    delValue_ = delValue;
    tableInfo_ = tableInfo;
  }

  @Override
  public void init() throws Exception {
    RID rid = tableInfo_.getIndex().getValue(delValue_, getCtx().getTransaction());
    if (rid == null) {
      return;
    }
    Transaction txn = getCtx().getTransaction();
    LockManager lockManager = getCtx().getLockManager();

    TransactionManager transactionManager = getCtx().getTransactionManager();
    transactionManager.makeDeletionLog(txn, rid, new Tuple(tableInfo_.getTable().getTuple(rid)));

    lockManager.lockTable(txn, LockManager.LockMode.INTENTION_EXCLUSIVE, tableInfo_.getTableName());
    lockManager.lockRow(txn, LockManager.LockMode.EXCLUSIVE, tableInfo_.getTableName(), rid);

    tableInfo_.getTable().delete(rid);
    tableInfo_.getIndex().remove(rid, getCtx().getTransaction());
  }

  @Override
  public boolean next(Tuple tuple, RID rid) throws Exception {
    return false;
  }

  @Override
  public Schema getOutputSchema() {
    return tableInfo_.getSchema();
  }
}
