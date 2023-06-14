package cn.edu.thssdb.storage.index;

import cn.edu.thssdb.buffer.BufferPoolManager;
import cn.edu.thssdb.concurrency.Transaction;
import cn.edu.thssdb.storage.Page;
import cn.edu.thssdb.type.Type;
import cn.edu.thssdb.type.Value;
import cn.edu.thssdb.utils.Global;
import cn.edu.thssdb.utils.Pair;
import cn.edu.thssdb.utils.RID;

import java.io.IOException;
import java.util.Iterator;

// B+ tree manages pages in a tree structure
// and provides search, insert, delete operations
public class BPlusTree implements Iterable<RID> {
  // root page can change after insert/delete operations
  private int rootPageId;
  private final BufferPoolManager bpm_;
  private final Type keyType;
  public static final int MAXSIZE_DECIDE_BY_PAGE = -1;
  // NOTE: for B+ tree, leafSize may be different from internalSize
  private final int leafSize;
  private final int internalSize;

  // open an existed B+ tree
  public BPlusTree(int rootPageId, BufferPoolManager bpm, Type keyType) {
    this.rootPageId = rootPageId;
    this.bpm_ = bpm;
    this.keyType = keyType;
    leafSize = MAXSIZE_DECIDE_BY_PAGE;
    internalSize = MAXSIZE_DECIDE_BY_PAGE;
  }

  public BPlusTree(
      int rootPageId, BufferPoolManager bpm, Type keyType, int leafSize, int internalSize) {
    this.rootPageId = rootPageId;
    this.bpm_ = bpm;
    this.keyType = keyType;
    this.leafSize = leafSize;
    this.internalSize = internalSize;
  }

  // new b+ tree
  public BPlusTree(BufferPoolManager bpm, Type keyType) {
    this.rootPageId = Global.PAGE_ID_INVALID;
    this.bpm_ = bpm;
    this.keyType = keyType;
    leafSize = MAXSIZE_DECIDE_BY_PAGE;
    internalSize = MAXSIZE_DECIDE_BY_PAGE;
  }

  public BPlusTree(BufferPoolManager bpm, Type keyType, int leafSize, int internalSize) {
    this.rootPageId = Global.PAGE_ID_INVALID;
    this.bpm_ = bpm;
    this.keyType = keyType;
    this.leafSize = leafSize;
    this.internalSize = internalSize;
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
    leafRoot.init(Global.PAGE_ID_INVALID, leafSize);
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

    // split after leaf reaches MAX
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
      siblingLeaf.init(leaf.getParentPageId(), leafSize);
      leaf.moveHalfTo(siblingLeaf);
      siblingLeaf.setNextPageId(leaf.getNextPageId());
      leaf.setNextPageId(siblingLeaf.getPageId());
    } else if (leafPage.getPageType() == BPlusTreePage.BTNodeType.INTERNAL) {
      sibling = new InternalNodePage(newPage, keyType);
      InternalNodePage siblingInternal = (InternalNodePage) sibling;
      InternalNodePage internal = (InternalNodePage) leafPage;
      siblingInternal.init(internal.getParentPageId(), internalSize);
      // fetch child, change their parent id
      int moveSize = internal.getCurrentSize() - internal.getCurrentSize() / 2;
      int start = internal.getCurrentSize() - moveSize;
      for (int i = 0; i < moveSize; i++) {
        Page child = bpm_.fetchPage(internal.getPointer(i + start));
        if (child == null) {
          throw new IOException("Failed to fetch child page for split");
        }
        BPlusTreePage bpage = new BPlusTreePage(child, keyType);
        bpage.setParentPageId(siblingInternal.getPageId());
        bpm_.unpinPage(child.getPageId(), true);
      }
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
      newRoot.init(Global.PAGE_ID_INVALID, internalSize);
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

  public void remove(Value<?, ?> key, Transaction txn) throws IOException {
    Page page = findLeafPage(key, false);
    LeafPage leaf = new LeafPage(page, keyType);
    leaf.deleteRecord(key);
    adjustTree(leaf, txn);
  }

  // adjust the tree structure
  //
  private void adjustTree(BPlusTreePage curNode, Transaction txn) throws IOException {
    if (curNode.isRootPage()) {
      adjustRoot(curNode);
      return;
    }

    // ===---------------------===
    // no problem scenario
    // ===---------------------===
    if (!curNode.isUnderflow()) {
      bpm_.unpinPage(curNode.getPageId(), true);
      return;
    }

    // ===---------------------===
    // check if we can coalesce
    // ===---------------------===

    // fetch siblings
    Page parentPage = bpm_.fetchPage(curNode.getParentPageId());
    if (parentPage == null) {
      throw new IOException("Failed to fetch parent page");
    }
    InternalNodePage parentInNode = new InternalNodePage(parentPage, keyType);
    int curIdx = parentInNode.findPointerIndex(curNode.getPageId());
    Pair<Integer, Integer> siblings = new Pair<>(curIdx - 1, curIdx + 1);

    // find left child, see if we can coalesce
    int leftSiblingIndex = siblings.left;
    if (leftSiblingIndex >= 0) {
      int lSPageId = parentInNode.getPointer(leftSiblingIndex);
      Page lSPage = bpm_.fetchPage(lSPageId);
      if (lSPage == null) {
        throw new IOException("Failed to fetch left sibling page");
      }
      BPlusTreePage leftSibling = new BPlusTreePage(lSPage, keyType);
      if (canCoalesce(leftSibling, curNode)) {
        coalesce(leftSibling, curNode, parentInNode, curIdx, txn);
        bpm_.unpinPage(lSPageId, true);
        bpm_.unpinPage(parentInNode.getPageId(), true);
        return;
      }
      bpm_.unpinPage(lSPageId, false);
    }

    int rightSiblingIndex = siblings.right;
    if (rightSiblingIndex < parentInNode.getCurrentSize()) {
      int rSPageId = parentInNode.getPointer(rightSiblingIndex);
      Page rSPage = bpm_.fetchPage(rSPageId);
      if (rSPage == null) {
        throw new IOException("Failed to fetch right sibling page");
      }
      BPlusTreePage rightSibling = new BPlusTreePage(rSPage, keyType);
      if (canCoalesce(curNode, rightSibling)) {
        // I believe need +1...
        coalesce(curNode, rightSibling, parentInNode, curIdx + 1, txn);
        bpm_.unpinPage(rSPageId, true);
        bpm_.unpinPage(parentInNode.getPageId(), true);
        return;
      }
      bpm_.unpinPage(rSPageId, false);
    }

    // ===---------------------===
    // check if we can redistribute
    // ===----------------------===

    // borrow one element from my left sibling
    if (leftSiblingIndex >= 0) {
      int lSPageId = parentInNode.getPointer(leftSiblingIndex);
      Page lSPage = bpm_.fetchPage(lSPageId);
      if (lSPage == null) {
        throw new IOException("Failed to fetch left sibling page");
      }
      BPlusTreePage leftSibling = new BPlusTreePage(lSPage, keyType);
      // if page is not a leaf
      if (leftSibling.getPageType() == BPlusTreePage.BTNodeType.INTERNAL) {
        InternalNodePage leftSiblingIntP = new InternalNodePage(leftSibling, keyType);
        int mIdx = leftSiblingIntP.getCurrentSize();
        // get the borrowed key and pointer!
        int pointerToBorrow = leftSiblingIntP.getPointer(mIdx);
        Value<?, ?> keyToBorrow = leftSiblingIntP.getKey(mIdx);
        leftSiblingIntP.deleteRecord(mIdx - 1);
        InternalNodePage curNodeIntP = new InternalNodePage(curNode, keyType);
        // pointer and parent key given to curNode
        curNodeIntP.pushFront(pointerToBorrow, parentInNode.getKey(curIdx));
        // parent key get reparation from left sibling
        parentInNode.setKey(curIdx, keyToBorrow);
      } else {
        LeafPage leftSiblingLeafP = new LeafPage(leftSibling, keyType);
        int mIdx = leftSiblingLeafP.getCurrentSize();
        // get the borrowed key and rid!
        Value<?, ?> keyToBorrow = leftSiblingLeafP.getKey(mIdx);
        RID ridToBorrow = leftSiblingLeafP.getRID(mIdx);
        leftSiblingLeafP.deleteRecord(mIdx - 1);
        LeafPage curNodeLeafP = new LeafPage(curNode, keyType);
        // pointer and parent key given to curNode
        curNodeLeafP.pushFront(ridToBorrow, parentInNode.getKey(curIdx));
        // parent key get reparation from left sibling
        parentInNode.setKey(curIdx, keyToBorrow);
      }
    } else if (rightSiblingIndex < parentInNode.getCurrentSize()) {
      int rSPageId = parentInNode.getPointer(rightSiblingIndex);
      Page rSPage = bpm_.fetchPage(rSPageId);
      if (rSPage == null) {
        throw new IOException("Failed to fetch right sibling page");
      }
      BPlusTreePage rightSibling = new BPlusTreePage(rSPage, keyType);
      // if page is not a leaf
      if (rightSibling.getPageType() == BPlusTreePage.BTNodeType.INTERNAL) {
        InternalNodePage rightSiblingIntP = new InternalNodePage(rightSibling, keyType);
        // get the borrowed key and pointer!
        int pointerToBorrow = rightSiblingIntP.getPointer(0);
        Value<?, ?> keyToBorrow = rightSiblingIntP.getKey(1); // 0 is unused
        rightSiblingIntP.deleteRecord(0);
        InternalNodePage curNodeIntP = new InternalNodePage(curNode, keyType);
        // pointer and parent key given to curNode
        // plus one: same reason as above (colaesce scenario)
        curNodeIntP.pushBack(pointerToBorrow, parentInNode.getKey(curIdx + 1));
        // parent key get reparation from left sibling
        parentInNode.setKey(curIdx + 1, keyToBorrow);
      } else {
        LeafPage rightSiblingLeafP = new LeafPage(rightSibling, keyType);
        // get the borrowed key and rid!
        Value<?, ?> keyToBorrow = rightSiblingLeafP.getKey(0);
        RID ridToBorrow = rightSiblingLeafP.getRID(0);
        rightSiblingLeafP.deleteRecord(0);
        LeafPage curNodeLeafP = new LeafPage(curNode, keyType);
        // pointer and parent key given to curNode
        curNodeLeafP.pushBack(ridToBorrow, parentInNode.getKey(curIdx));
        // parent key get reparation from left sibling
        parentInNode.setKey(curIdx, keyToBorrow);
      }
    }
  }

  void adjustRoot(BPlusTreePage oldRoot) throws IOException {
    if (oldRoot.getPageType() == BPlusTreePage.BTNodeType.INTERNAL
        && oldRoot.getCurrentSize() == 1) {
      InternalNodePage oldRootIntP = new InternalNodePage(oldRoot, keyType);
      int newPageId = oldRootIntP.getPointer(0);
      Page newPage = bpm_.fetchPage(newPageId);
      if (newPage == null) {
        throw new IOException("Failed to fetch new root page");
      }
      BPlusTreePage newRoot = new BPlusTreePage(newPage, keyType);
      newRoot.setParentPageId(Global.PAGE_ID_INVALID);
      setRootPageId(newRoot.getPageId());
      bpm_.unpinPage(newPageId, true);
      return;
    }
    if (oldRoot.getPageType() == BPlusTreePage.BTNodeType.LEAF && oldRoot.getCurrentSize() == 0) {
      setRootPageId(Global.PAGE_ID_INVALID);
    }
  }

  boolean canCoalesce(BPlusTreePage leftPg, BPlusTreePage rightPg) {
    return leftPg.getCurrentSize() + rightPg.getCurrentSize() < leftPg.getMaxSize();
  }

  // coalesce always merge right to ;eft
  // ensures left is predecessor of right
  private void coalesce(
      BPlusTreePage leftSibling,
      BPlusTreePage curNode,
      InternalNodePage parentInNode,
      int curIdx,
      Transaction txn)
      throws IOException {
    if (leftSibling.getPageType() == BPlusTreePage.BTNodeType.LEAF) {
      LeafPage leafLeftNode = new LeafPage(leftSibling, keyType);
      LeafPage leafRightNode = new LeafPage(curNode, keyType);
      leafRightNode.moveAllTo(leafLeftNode);
      leafLeftNode.setNextPageId(leafRightNode.getNextPageId());
    } else { // internal page
      InternalNodePage internalLeftNode = new InternalNodePage(leftSibling, keyType);
      InternalNodePage internalRightNode = new InternalNodePage(curNode, keyType);
      // also need a key from father
      Value<?, ?> fallenKey = parentInNode.getKey(curIdx);
      internalRightNode.moveAllTo(internalLeftNode, fallenKey);
    }

    // delete the pointer in parent
    bpm_.unpinPage(curNode.getPageId(), true);
    parentInNode.deleteRecord(curIdx);
    if (parentInNode.isUnderflow()) {
      adjustTree(parentInNode, txn);
    }
  }

  // point search
  public RID getValue(Value<?, ?> key) throws IOException {
    if (rootPageId == Global.PAGE_ID_INVALID) {
      return null;
    }

    // find leaf page
    Page page = findLeafPage(key, false);
    if (page == null) {
      throw new IOException("Failed to fetch leaf page");
    }
    LeafPage leafPage = new LeafPage(page, keyType);
    return leafPage.lookUp(key);
  }

  public String toJson() throws IOException {
    if (rootPageId == Global.PAGE_ID_INVALID) {
      return "[]";
    }
    Page page = bpm_.fetchPage(rootPageId);
    if (page == null) {
      throw new IOException("Failed to fetch B+ tree root page");
    }
    BPlusTreePage bpage = new BPlusTreePage(page, keyType);
    return bpage.toJsonBPTP(bpm_).toString();
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

  @Override
  public Iterator<RID> iterator() {
    try {
      return iterator(null);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public Iterator<RID> iterator(Value<?, ?> key) throws IOException {
    if (key == null) {
      Page page = findLeafPage(null, true);
      LeafPage leaf = new LeafPage(page, keyType);
      return new BPlusTreeIterator(leaf, bpm_, 0);
    }
    Page page = findLeafPage(key, false);
    if (page == null) {
      return null;
    }
    LeafPage leaf = new LeafPage(page, keyType);
    int index = leaf.getKeyIndex(key);
    if (index == -1) {
      return null;
    }
    return new BPlusTreeIterator(leaf, bpm_, index);
  }
}
