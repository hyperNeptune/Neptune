package neptune.backend.storage.index;

import neptune.backend.buffer.BufferPoolManager;
import neptune.backend.concurrency.Transaction;
import neptune.backend.storage.Page;
import neptune.backend.type.Type;
import neptune.backend.type.Value;
import neptune.common.Global;
import neptune.common.Pair;
import neptune.common.RID;

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.locks.ReentrantLock;

// B+ tree manages pages in a tree structure
// and provides search, insert, delete operations
public class BPlusTree implements Iterable<Pair<Value<?, ?>, RID>> {
  // root page can change after insert/delete operations
  private int rootPageId;
  private final BufferPoolManager bpm_;
  private final Type keyType;
  public static final int MAXSIZE_DECIDE_BY_PAGE = -1;
  // NOTE: for B+ tree, leafSize may be different from internalSize
  private final int leafSize;
  private final int internalSize;
  private final ReentrantLock oneGiantEvilLock = new ReentrantLock(true); // for test purpose only

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

  // page is pinned after this function
  private Page findLeafPage(Value<?, ?> keyValue, boolean leftMost) throws IOException {
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

  private boolean insertToLeaf(Value<?, ?> keyValue, RID rid, Transaction txn) throws IOException {
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
      // sibling is pinned
      LeafPage sibling = (LeafPage) split(leaf);
      // insert the new key to parent
      insertToParent(leaf, sibling.getKey(0), sibling, txn);
    }
    bpm_.unpinPage(leafPage.getPageId(), true);
    return true;
  }

  // return page is pinned
  private BPlusTreePage split(BPlusTreePage leafPage) throws IOException {
    Page newPage = bpm_.newPage();
    if (newPage == null) {
      throw new IOException("Failed to allocate new page for split");
    }
    BPlusTreePage sibling;
    if (leafPage.getPageType() == BPlusTreePage.BTNodeType.LEAF) {
      LeafPage leaf = (LeafPage) leafPage;
      sibling = new LeafPage(newPage, keyType);
      LeafPage siblingLeaf = (LeafPage) sibling;
      siblingLeaf.init(leaf.getParentPageId(), leafSize);
      leaf.moveHalfTo(siblingLeaf);
      siblingLeaf.setNextPageId(leaf.getNextPageId());
      leaf.setNextPageId(siblingLeaf.getPageId());
      return sibling;
    }
    // internal node
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
      bpm_.unpinPage(right.getPageId(), true);
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
    oneGiantEvilLock.lock();
    // System.out.println("inserting " + key);
    boolean ret = insertToLeaf(key, rid, txn);
    oneGiantEvilLock.unlock();
    //    System.out.println("inserted " + key);
    return ret;
  }

  public void remove(Value<?, ?> key, Transaction txn) throws IOException {
    oneGiantEvilLock.lock();
    //    System.out.println("removing " + key);
    Page page = findLeafPage(key, false);
    LeafPage leaf = new LeafPage(page, keyType);
    // leaf is pinned
    leaf.deleteRecord(key);
    adjustTree(leaf, txn);
    // should all unpinned
    oneGiantEvilLock.unlock();
    //    System.out.println("removed " + key);
  }

  // ====----------------------===
  // adjust the tree structure
  // ====----------------------===

  // this function unpin all
  private void adjustTree(BPlusTreePage curNode, Transaction txn) throws IOException {
    if (curNode.isRootPage()) {
      adjustRoot(curNode);
      bpm_.unpinPage(curNode.getPageId(), true);
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
    if (curIdx >= parentInNode.getCurrentSize()) {
      throw new IOException("[CONCURRENCY PROBLEM] Failed to find current node in parent");
    }

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
        bpm_.unpinPage(curNode.getPageId(), true);
        bpm_.unpinPage(parentInNode.getPageId(), true);
        return;
      }
      bpm_.unpinPage(rSPageId, false);
    }

    // ===---------------------===
    // check if we can redistribute
    // ===----------------------===

    // parent, curNode is pinned
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
        int pointerToBorrow = leftSiblingIntP.getPointer(mIdx - 1);
        Value<?, ?> keyToBorrow = leftSiblingIntP.getKey(mIdx - 1);
        leftSiblingIntP.deleteRecord(mIdx - 1);
        InternalNodePage curNodeIntP = new InternalNodePage(curNode, keyType);
        // pointer and parent key given to curNode
        curNodeIntP.pushFront(pointerToBorrow, parentInNode.getKey(curIdx));
        // and its parent is changed
        BPlusTreePage pChangeParent = new BPlusTreePage(bpm_.fetchPage(pointerToBorrow), keyType);
        pChangeParent.setParentPageId(curNodeIntP.getPageId());
        bpm_.unpinPage(pointerToBorrow, true);
        // parent key get reparation from left sibling
        parentInNode.setKey(curIdx, keyToBorrow);
      } else { // is leaf
        LeafPage leftSiblingLeafP = new LeafPage(leftSibling, keyType);
        int mIdx = leftSiblingLeafP.getCurrentSize();
        // get the borrowed key and rid!
        Value<?, ?> keyToBorrow = leftSiblingLeafP.getKey(mIdx - 1);
        RID ridToBorrow = leftSiblingLeafP.getRID(mIdx - 1);
        leftSiblingLeafP.deleteRecord(mIdx - 1);
        LeafPage curNodeLeafP = new LeafPage(curNode, keyType);
        // pointer and parent key given to curNode
        curNodeLeafP.pushFront(ridToBorrow, keyToBorrow);
        // parent key get reparation from left sibling
        parentInNode.setKey(curIdx, keyToBorrow);
      }
      bpm_.unpinPage(lSPageId, true);
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
        // plus one: same reason as above (coalesce scenario)
        curNodeIntP.pushBack(pointerToBorrow, parentInNode.getKey(rightSiblingIndex));
        BPlusTreePage pChangedParent = new BPlusTreePage(bpm_.fetchPage(pointerToBorrow), keyType);
        pChangedParent.setParentPageId(curNodeIntP.getPageId());
        bpm_.unpinPage(pointerToBorrow, true);
        // parent key get reparation from left sibling
        parentInNode.setKey(rightSiblingIndex, keyToBorrow);
      } else {
        LeafPage rightSiblingLeafP = new LeafPage(rightSibling, keyType);
        // get the borrowed key and rid!
        Value<?, ?> keyToBorrow = rightSiblingLeafP.getKey(0);
        RID ridToBorrow = rightSiblingLeafP.getRID(0);
        rightSiblingLeafP.deleteRecord(0);
        LeafPage curNodeLeafP = new LeafPage(curNode, keyType);
        // pointer and parent key given to curNode
        curNodeLeafP.pushBack(ridToBorrow, keyToBorrow);
        // parent key get reparation from left sibling
        parentInNode.setKey(rightSiblingIndex, keyToBorrow);
      }
      bpm_.unpinPage(rSPageId, true);
    }
    bpm_.unpinPage(parentPage.getPageId(), true);
    bpm_.unpinPage(curNode.getPageId(), true);
  }

  private void adjustRoot(BPlusTreePage oldRoot) throws IOException {
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

  private boolean canCoalesce(BPlusTreePage leftPg, BPlusTreePage rightPg) {
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
      if (fallenKey == null) {
        throw new IOException("Failed to get fallen key");
      }
      // change the parent pointer of all children of right node
      for (int i = 0; i < internalRightNode.getCurrentSize(); i++) {
        int childPageId = internalRightNode.getPointer(i);
        Page childPage = bpm_.fetchPage(childPageId);
        if (childPage == null) {
          throw new IOException("Failed to fetch child page");
        }
        BPlusTreePage childNode = new BPlusTreePage(childPage, keyType);
        childNode.setParentPageId(internalLeftNode.getPageId());
        bpm_.unpinPage(childPageId, true);
      }
      internalRightNode.moveAllTo(internalLeftNode, fallenKey);
    }

    // delete the pointer in parent
    bpm_.unpinPage(curNode.getPageId(), true);
    parentInNode.deleteRecord(curIdx);
    if (parentInNode.isUnderflow()) {
      adjustTree(parentInNode, txn);
    }
  }

  // for test purpose only
  RID getValue(Value<?, ?> key) throws IOException {
    return getValue(key, null);
  }

  // point search
  public RID getValue(Value<?, ?> key, Transaction txn) throws IOException {
    oneGiantEvilLock.lock();
    if (rootPageId == Global.PAGE_ID_INVALID) {
      oneGiantEvilLock.unlock();
      return null;
    }

    // find leaf page
    Page page = findLeafPage(key, false);
    if (page == null) {
      oneGiantEvilLock.unlock();
      throw new IOException("Failed to fetch leaf page");
    }
    LeafPage leafPage = new LeafPage(page, keyType);
    RID rid = leafPage.lookUp(key);
    bpm_.unpinPage(leafPage.getPageId(), false);
    oneGiantEvilLock.unlock();
    return rid;
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
    String s = bpage.toJsonBPTP(bpm_).toString();
    bpm_.unpinPage(rootPageId, false);
    return s;
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
    bpm_.unpinPage(rootPageId, false);
  }

  @Override
  public Iterator<Pair<Value<?, ?>, RID>> iterator() {
    try {
      return iterator(null);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public Iterator<Pair<Value<?, ?>, RID>> iterator(Value<?, ?> key) throws IOException {
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

  public void remove(RID rid, Transaction txn) {
    // stupid method. iterate and find key, then call remove
    Value<?, ?> key = null;
    try {
      for (Pair<Value<?, ?>, RID> pcurRid : this) {
        RID curRid = pcurRid.right;
        key = pcurRid.left;
        if (curRid.equals(rid)) {
          remove(key, txn);
          return;
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
