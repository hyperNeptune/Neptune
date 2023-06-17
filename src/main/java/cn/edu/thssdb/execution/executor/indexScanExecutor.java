package cn.edu.thssdb.execution.executor;

import cn.edu.thssdb.concurrency.LockManager;
import cn.edu.thssdb.concurrency.Transaction;
import cn.edu.thssdb.execution.ExecContext;
import cn.edu.thssdb.parser.expression.BinaryExpression;
import cn.edu.thssdb.parser.expression.ConstantExpression;
import cn.edu.thssdb.parser.expression.Expression;
import cn.edu.thssdb.parser.tableBinder.RegularTableBinder;
import cn.edu.thssdb.parser.tableBinder.TableBinder;
import cn.edu.thssdb.schema.Schema;
import cn.edu.thssdb.schema.TableInfo;
import cn.edu.thssdb.storage.Tuple;
import cn.edu.thssdb.type.Value;
import cn.edu.thssdb.utils.Pair;
import cn.edu.thssdb.utils.RID;

import java.util.Iterator;

public class indexScanExecutor extends Executor {
  TableBinder from_;
  Expression where_;
  Schema schema_;
  TableInfo tableInfo_;
  boolean drain_ = false;
  Iterator<Pair<Value<?, ?>, RID>> iterator_ = null; // for range query
  RID targetRID_ = null; // for point query

  enum scanType {
    EQUAL,
    LESS,
    GREATER,
    LESS_EQUAL,
    GREATER_EQUAL,
    NOT_EQUAL
  }

  scanType scanType_;
  ConstantExpression constantExpression_ = null;

  public indexScanExecutor(ExecContext ctx, TableBinder from, Expression where) {
    super(ctx);
    from_ = from;
    where_ = where;
    tableInfo_ = ((RegularTableBinder) from_).getTableInfo();
    schema_ = tableInfo_.getSchema();
  }

  // 1. for range query: build iterators
  // 2. for point query: build targetRID
  @Override
  public void init() throws Exception {
    drain_ = false;
    BinaryExpression binaryExpression = (BinaryExpression) where_;
    if (binaryExpression == null) {
      throw new Exception("where expression is not binary expression");
    }

    Transaction txn = getCtx().getTransaction();
    LockManager lockManager = getCtx().getLockManager();
    lockManager.lockTable(txn, LockManager.LockMode.INTENTION_SHARED, tableInfo_.getTableName());

    constantExpression_ = (ConstantExpression) binaryExpression.getRight();
    switch (binaryExpression.getOp()) {
      case "eq":
        scanType_ = scanType.EQUAL;
        targetRID_ =
            tableInfo_
                .getIndex()
                .getValue(constantExpression_.evaluation(null, null), getCtx().getTransaction());
        break;
      case "lt":
        scanType_ = scanType.LESS;
        iterator_ = tableInfo_.getIndex().iterator();
        break;
      case "gt":
        scanType_ = scanType.GREATER;
        iterator_ = tableInfo_.getIndex().iterator(constantExpression_.evaluation(null, null));
        break;
      case "le":
        scanType_ = scanType.LESS_EQUAL;
        iterator_ = tableInfo_.getIndex().iterator();
        break;
      case "ge":
        scanType_ = scanType.GREATER_EQUAL;
        iterator_ = tableInfo_.getIndex().iterator(constantExpression_.evaluation(null, null));
        break;
      case "ne":
        scanType_ = scanType.NOT_EQUAL;
        iterator_ = tableInfo_.getIndex().iterator();
        break;
      default:
        throw new Exception("unsupported operator");
    }
  }

  @Override
  public boolean next(Tuple tuple, RID rid) throws Exception {
    if (drain_) {
      return false;
    }
    if (scanType_ == scanType.EQUAL) {
      if (targetRID_ == null) {
        return false;
      }

      LockManager lockManager = getCtx().getLockManager();
      Transaction txn = getCtx().getTransaction();
      lockManager.lockRow(txn, LockManager.LockMode.SHARED, tableInfo_.getTableName(), targetRID_);

      rid.assign(targetRID_);
      tuple.copyAssign(tableInfo_.getTable().getTuple(rid));
      drain_ = true;
      return true;
    }
    if (scanType_ == scanType.LESS) {
      if (!iterator_.hasNext()) {
        return false;
      }
      Pair<Value<?, ?>, RID> pair = iterator_.next();
      RID rid1 = pair.right;
      Tuple tuple1 = tableInfo_.getTable().getTuple(rid1);
      Value<?, ?> nowValue = pair.left;
      if (nowValue.compareTo(constantExpression_.evaluation(null, null)) >= 0) {
        drain_ = true;
        return false;
      }
      rid.assign(rid);
      tuple.copyAssign(tuple1);
      return true;
    }
    if (scanType_ == scanType.GREATER) {
      if (!iterator_.hasNext()) {
        return false;
      }
      Pair<Value<?, ?>, RID> pair = iterator_.next();
      RID rid1 = pair.right;
      Tuple tuple1 = tableInfo_.getTable().getTuple(rid1);
      Value<?, ?> nowValue = pair.left;
      if (nowValue.compareTo(constantExpression_.evaluation(null, null)) == 0) {
        if (!iterator_.hasNext()) {
          return false;
        }
        rid1 = iterator_.next().right;
        tuple1 = tableInfo_.getTable().getTuple(rid1);
      }
      rid.assign(rid);
      tuple.copyAssign(tuple1);
      return true;
    }
    if (scanType_ == scanType.LESS_EQUAL) {
      if (!iterator_.hasNext()) {
        return false;
      }
      Pair<Value<?, ?>, RID> pair = iterator_.next();
      RID rid1 = pair.right;
      Tuple tuple1 = tableInfo_.getTable().getTuple(rid1);
      Value<?, ?> nowValue = pair.left;
      if (nowValue.compareTo(constantExpression_.evaluation(null, null)) > 0) {
        drain_ = true;
        return false;
      }
      rid.assign(rid);
      tuple.copyAssign(tuple1);
      return true;
    }
    if (scanType_ == scanType.GREATER_EQUAL) {
      if (!iterator_.hasNext()) {
        return false;
      }
      Pair<Value<?, ?>, RID> pair = iterator_.next();
      RID rid1 = pair.right;
      Tuple tuple1 = tableInfo_.getTable().getTuple(rid1);
      rid.assign(rid);
      tuple.copyAssign(tuple1);
      return true;
    }
    if (scanType_ == scanType.NOT_EQUAL) {
      if (!iterator_.hasNext()) {
        return false;
      }
      Pair<Value<?, ?>, RID> pair = iterator_.next();
      RID rid1 = pair.right;
      Tuple tuple1 = tableInfo_.getTable().getTuple(rid1);
      Value<?, ?> nowValue = pair.left;
      if (nowValue.compareTo(constantExpression_.evaluation(null, null)) == 0) {
        if (!iterator_.hasNext()) {
          return false;
        }
        rid1 = iterator_.next().right;
        tuple1 = tableInfo_.getTable().getTuple(rid1);
      }
      rid.assign(rid);
      tuple.copyAssign(tuple1);
      return true;
    }
    throw new Exception("unsupported operator");
  }

  @Override
  public Schema getOutputSchema() {
    return schema_;
  }

  @Override
  public String toString() {
    return "indexScanExecutor";
  }
}
