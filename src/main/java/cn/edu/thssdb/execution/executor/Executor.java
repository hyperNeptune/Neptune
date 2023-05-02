package cn.edu.thssdb.execution.executor;

import cn.edu.thssdb.execution.ExecContext;
import cn.edu.thssdb.schema.Schema;
import cn.edu.thssdb.storage.Tuple;
import cn.edu.thssdb.utils.RID;

// VOLCANO MODEL
public abstract class Executor {
  // all information is encoded in the ctx
  private ExecContext ctx_;

  // initialize the executor
  public abstract void init() throws Exception;

  // the parameters tuple, rid are [OUTPUT PARAMETERS]!!
  // return false if there is no more tuple
  public abstract boolean next(Tuple tuple, RID rid);

  public abstract Schema getOutputSchema();

  public ExecContext getCtx() {
    return ctx_;
  }

}
