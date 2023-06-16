package cn.edu.thssdb.storage;

import java.util.concurrent.locks.ReadWriteLock;

// for mutable runtime data
public class PageRuntimeData {
  protected boolean is_dirty_;
  protected int pin_count_;
  protected ReadWriteLock RWLatch_;
}
