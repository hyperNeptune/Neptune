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
  public static final int MAXSIZE_DECIDE_BY_PAGE = -1;
  private final int maxSize;

  // open an existed B+ tree
  public BPlusTree(int rootPageId, BufferPoolManager bpm, Type keyType) {
    this.rootPageId = rootPageId;
    this.bpm_ = bpm;
    this.keyType = keyType;
    maxSize = MAXSIZE_DECIDE_BY_PAGE;
  }

  public BPlusTree(int rootPageId, BufferPoolManager bpm, Type keyType, int maxSize) {
    this.rootPageId = rootPageId;
    this.bpm_ = bpm;
    this.keyType = keyType;
    this.maxSize = maxSize;
  }

  // new b+ tree
  public BPlusTree(BufferPoolManager bpm, Type keyType) {
    this.rootPageId = Global.PAGE_ID_INVALID;
    this.bpm_ = bpm;
    this.keyType = keyType;
    maxSize = MAXSIZE_DECIDE_BY_PAGE;
  }

  public BPlusTree(BufferPoolManager bpm, Type keyType, int maxSize) {
    this.rootPageId = Global.PAGE_ID_INVALID;
    this.bpm_ = bpm;
    this.keyType = keyType;
    this.maxSize = maxSize;
  }

  // getter
  public int getRootPageId() {
    return rootPageId;
  }

  public void setRootPageId(int rootPageId) {
    this.rootPageId = rootPageId;
  }

  public boolean isEmpty() throws IOException {
    return rootPageId == Global.PAGE_ID_INVALID;
  }

  // start a new B+ tree
  void createTree(Value<?, ?> v, RID rid) throws IOException {
    Page rootPage = bpm_.newPage();
    if (rootPage == null) {
      throw new IOException("Failed to create B+ tree root page");
    }
    LeafPage leafRoot = new LeafPage(rootPage, keyType);
    leafRoot.init(Global.PAGE_ID_INVALID, maxSize);
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
      LeafPage sibling = (LeafPage) split(leaf);
      // insert the new key to parent
      assert sibling != null;
      insertToParent(leaf, sibling.getKey(0), sibling, txn);
    }
    bpm_.unpinPage(leafPage.getPageId(), true);
    return true;
  }

  private BPlusTreePage split(BPlusTreePage leafPage) throws IOException {
    Page newPage = bpm_.newPage();
    if (newPage == null) {
      throw new IOException("Failed to allocate new page for split");
    }
    BPlusTreePage sibling = null;
    if (leafPage.getPageType() == BPlusTreePage.BTNodeType.LEAF) {
      LeafPage leaf = (LeafPage) leafPage;
      sibling = new LeafPage(newPage, keyType);
      LeafPage siblingLeaf = (LeafPage) sibling;
      siblingLeaf.init(leaf.getParentPageId(), maxSize);
      leaf.moveHalfTo(siblingLeaf);
      siblingLeaf.setNextPageId(leaf.getNextPageId());
      leaf.setNextPageId(siblingLeaf.getPageId());
    } else if (leafPage.getPageType() == BPlusTreePage.BTNodeType.INTERNAL) {
      sibling = new InternalNodePage(newPage, keyType);
      InternalNodePage siblingInternal = (InternalNodePage) sibling;
      InternalNodePage internal = (InternalNodePage) leafPage;
      siblingInternal.init(internal.getParentPageId(), maxSize);
      internal.moveHalfTo(siblingInternal);
    }
    return sibling;
  }

  private void insertToParent(
      BPlusTreePage left, Value<?, ?> key, BPlusTreePage right, Transaction txn)
      throws IOException {
    if (left.isRootPage()) {
      // create new root
      Page newRootPage = bpm_.newPage();
      if (newRootPage == null) {
        throw new RuntimeException("Failed to allocate new page for new root");
      }
      InternalNodePage newRoot = new InternalNodePage(newRootPage, keyType);
      newRoot.init(Global.PAGE_ID_INVALID, maxSize);
      newRoot.setPointer(0, left.getPageId());
      newRoot.setKey(1, key);
      newRoot.setPointer(1, right.getPageId());
      newRoot.increaseSize(2);
      setRootPageId(newRoot.getPageId());
      left.setParentPageId(newRoot.getPageId());
      right.setParentPageId(newRoot.getPageId());
      bpm_.unpinPage(newRootPage.getPageId(), true);
    } else {
      // insert to parent
      Page parentPage = bpm_.fetchPage(left.getParentPageId());
      if (parentPage == null) {
        throw new RuntimeException("Failed to fetch parent page");
      }
      InternalNodePage parent = new InternalNodePage(parentPage, keyType);
      parent.insertAfter(left.getPageId(), key, right.getPageId());
      if (parent.getCurrentSize() == parent.getMaxSize()) {
        InternalNodePage sibling = (InternalNodePage) split(parent);
        insertToParent(parent, sibling.getKey(0), sibling, txn);
        bpm_.unpinPage(sibling.getPageId(), true);
      }
      bpm_.unpinPage(parent.getPageId(), true);
    }
  }

  public boolean insert(Value<?, ?> key, RID rid, Transaction txn) throws IOException {
    return insertToLeaf(key, rid, txn);
  }

  public void remove(Value<?, ?> key, Transaction txn) {}

  public RID getValue(Value<?, ?> key) {
    return null;
  }

  public void print() throws IOException {
    if (rootPageId == Global.PAGE_ID_INVALID) {
      System.out.println("B+ tree is empty");
      return;
    }
    Page page = bpm_.fetchPage(rootPageId);
    if (page == null) {
      throw new IOException("Failed to fetch B+ tree root page");
    }
    BPlusTreePage bpage = new BPlusTreePage(page, keyType);
    bpage.print();
    if (bpage.getPageType() == BPlusTreePage.BTNodeType.INTERNAL) {
      InternalNodePage internalNodePage = new InternalNodePage(bpage, keyType);
      internalNodePage.print();
    }
  }
}
