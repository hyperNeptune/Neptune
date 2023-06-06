package cn.edu.thssdb.storage.index;

import cn.edu.thssdb.buffer.BufferPoolManager;
import cn.edu.thssdb.concurrency.Transaction;
import cn.edu.thssdb.storage.Page;
import cn.edu.thssdb.type.Type;
import cn.edu.thssdb.type.Value;
import cn.edu.thssdb.utils.Global;
import cn.edu.thssdb.utils.RID;

import java.io.IOException;

// B+ tree manages pages in a tree structure
// and provides search, insert, delete operations
public class BPlusTree {
  // root page can change after insert/delete operations
  private int rootPageId;
  private final BufferPoolManager bpm_;
  private final Type keyType;

  public BPlusTree(int rootPageId, BufferPoolManager bpm, Type keyType) {
    this.rootPageId = rootPageId;
    this.bpm_ = bpm;
    this.keyType = keyType;
  }

  // getter
  public int getRootPageId() {
    return rootPageId;
  }

  public void setRootPageId(int rootPageId) {
    this.rootPageId = rootPageId;
  }

  public boolean isEmpty() throws IOException {
    InternalNodePage root = (InternalNodePage) bpm_.fetchPage(rootPageId);
    return root.getCurrentSize() == 0;
  }

  // start a new B+ tree
  void createTree(Value<?, ?> v, RID rid) throws IOException {
    Page rootPage = bpm_.newPage();
    if (rootPage == null) {
      throw new IOException("Failed to create B+ tree root page");
    }
    LeafPage leafRoot = new LeafPage(rootPage, keyType);
    leafRoot.init(Global.PAGE_ID_INVALID);
    setRootPageId(leafRoot.getPageId());
    leafRoot.insert(v, rid);
    bpm_.unpinPage(rootPage.getPageId(), true);
  }

  public Page findLeafPage(Value<?, ?> keyValue, boolean leftMost) throws IOException {
    if (rootPageId == Global.PAGE_ID_INVALID) {
      throw new RuntimeException("B+ tree is empty");
    }
    Page page = bpm_.fetchPage(rootPageId);
    if (page == null) {
      throw new IOException("Failed to fetch B+ tree root page");
    }
    BPlusTreePage bpage = new BPlusTreePage(page, keyType);
    while (bpage.getPageType() == BPlusTreePage.BTNodeType.INTERNAL) {
      InternalNodePage internalNodePage = new InternalNodePage(bpage, keyType);
      int childPageId =
          leftMost ? internalNodePage.getPointer(0) : internalNodePage.lookUp(keyValue);
      bpm_.unpinPage(bpage.getPageId(), false);
      page = bpm_.fetchPage(childPageId);
      if (page == null) {
        throw new IOException("Failed to fetch B+ tree internal page");
      }
      bpage = new BPlusTreePage(page, keyType);
    }
    return page;
  }

  public boolean insertToLeaf(Value<?, ?> keyValue, RID rid, Transaction txn) throws IOException {
    if (isEmpty()) {
      createTree(keyValue, rid);
      return true;
    }
    Page leafPage = findLeafPage(keyValue, false);
    LeafPage leaf = new LeafPage(leafPage, keyType);
    if (leaf.lookUp(keyValue) != null) {
      // we don't allow duplicate keys
      bpm_.unpinPage(leafPage.getPageId(), false);
      return false;
    }
    leaf.insert(keyValue, rid);

    // split?
    if (leaf.getCurrentSize() == leaf.getMaxSize()) {
      // split
      LeafPage sibling = split(leaf);
      // insert the new key to parent
      assert sibling != null;
      insertToParent(leaf, sibling.getKey(0), sibling, txn);
    }
    bpm_.unpinPage(leafPage.getPageId(), true);
    return true;
  }

  private LeafPage split(LeafPage leafPage) {
    return null;
  }

  private void insertToParent(
      BPlusTreePage left, Value<?, ?> key, BPlusTreePage right, Transaction txn) {}

  public boolean insert(Value<?, ?> key, RID rid, Transaction txn) {
    return false;
  }

  public void remove(Value<?, ?> key, Transaction txn) {}

  public RID getValue(Value<?, ?> key) {
    return null;
  }
}
