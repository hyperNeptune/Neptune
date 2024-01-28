package neptune.backend.execution.executor;

import neptune.backend.execution.ExecContext;
import neptune.backend.schema.Schema;
import neptune.backend.storage.Tuple;
import neptune.common.RID;

// VOLCANO MODEL
// the executor is like a tree of iterators
// Exec Engine keep calling next() to the root executor, and root executor calls its children's
// next(), so on.
public abstract class Executor {
  // all information is encoded in the ctx
  private final ExecContext ctx_;

  public Executor(ExecContext ctx) {
    ctx_ = ctx;
  }

  // initialize the executor
  // note: init is different from constructor. for example, in sequential scan, constructor
  // tells the executor what table to execute, and init tells it to scan from the beginning.
  public abstract void init() throws Exception;

  // the parameters tuple, rid are [OUTPUT PARAMETERS]!!
  // return false if there is no more tuple
  public abstract boolean next(Tuple tuple, RID rid) throws Exception;

  public abstract Schema getOutputSchema();

  public ExecContext getCtx() {
    return ctx_;
  }
}
