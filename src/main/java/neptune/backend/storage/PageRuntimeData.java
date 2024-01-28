package neptune.backend.storage;

import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

// for mutable runtime data
public class PageRuntimeData {
  protected boolean is_dirty_;
  protected int pin_count_;
  protected ReadWriteLock RWLatch_ = new ReentrantReadWriteLock(true);
}
