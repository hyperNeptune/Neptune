package cn.edu.thssdb.storage.index;

import cn.edu.thssdb.buffer.BufferPoolManager;
import cn.edu.thssdb.type.Type;
import cn.edu.thssdb.type.Value;

// B+ tree manages pages in a tree structure
// and provides search, insert, delete operations
public class BPlusTree<K extends Value<? extends Type, ?>> {
  private final int rootPageId;
  private final BufferPoolManager bpm_;

  public BPlusTree(int rootPageId, BufferPoolManager bpm) {
    this.rootPageId = rootPageId;
    this.bpm_ = bpm;
  }

  // getter
  public int getRootPageId() {
    return rootPageId;
  }
}
