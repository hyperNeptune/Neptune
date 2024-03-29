package neptune.backend.storage.index;

import neptune.backend.storage.Page;
import neptune.backend.type.Type;
import neptune.backend.type.Value;
import neptune.common.Global;
import neptune.common.RID;

// leaf node page represents leaf nodes in B+ tree
// we are late materialized here. So leaf node stores `<key, RID>`
// it's like:
// | header | nextPageId | key1 | RID1 | key2 | RID2 | ... | keyN | RIDN |
public class LeafPage extends BPlusTreePage {
  public static final int NEXT_PAGE_ID_OFFSET = 0;
  public static final int LEAF_NODE_HEADER_SIZE = 4;
  public static final int ALL_PAGE_HEADER_SIZE =
      PAGE_HEADER_SIZE + B_PLUS_TREE_PAGE_HEADER_SIZE + LEAF_NODE_HEADER_SIZE;
  private final int pairSize = keyType.getTypeSize() + RID.getSize();

  // just convert, do nothing
  public LeafPage(Page page, Type keyType) {
    super(page, keyType);
  }

  public void init(int parentId) {
    setPageType(BTNodeType.LEAF);
    setParentPageId(parentId);
    setCurrentSize(0);
    setMaxSize((Global.PAGE_SIZE - ALL_PAGE_HEADER_SIZE) / pairSize);
    setNextPageId(Global.PAGE_ID_INVALID);
  }

  // for test purpose
  public void init(int parentId, int maxSize) {
    init(parentId);
    if (maxSize != BPlusTree.MAXSIZE_DECIDE_BY_PAGE) {
      setMaxSize(maxSize);
    }
  }

  // getters and setters
  public int getNextPageId() {
    return data_.getInt(PAGE_HEADER_SIZE + B_PLUS_TREE_PAGE_HEADER_SIZE + NEXT_PAGE_ID_OFFSET);
  }

  public void setNextPageId(int next_page_id) {
    data_.putInt(
        PAGE_HEADER_SIZE + B_PLUS_TREE_PAGE_HEADER_SIZE + NEXT_PAGE_ID_OFFSET, next_page_id);
  }

  @Override
  Value<?, ?> getKey(int index) {
    if (index >= getCurrentSize()) {
      return null;
    }
    int offset = ALL_PAGE_HEADER_SIZE + index * pairSize;
    return keyType.deserializeValue(data_, offset);
  }

  protected RID getRID(int index) {
    if (index >= getCurrentSize()) {
      return null;
    }
    int offset = ALL_PAGE_HEADER_SIZE + index * pairSize + keyType.getTypeSize();
    return new RID(data_.getInt(offset), data_.getInt(offset + 4));
  }

  // setter
  private void setKey(int index, Value<?, ?> key) {
    if (index >= getMaxSize()) {
      return;
    }
    int offset = ALL_PAGE_HEADER_SIZE + index * pairSize;
    key.serialize(data_, offset);
  }

  private void setRID(int index, RID rid) {
    if (index >= getMaxSize()) {
      return;
    }
    int offset = ALL_PAGE_HEADER_SIZE + index * pairSize + keyType.getTypeSize();
    data_.putInt(offset, rid.getPageId());
    data_.putInt(offset + 4, rid.getSlotId());
  }

  public RID lookUp(Value<?, ?> key) {
    int idx = getKeyIndex(key);
    if (idx < getCurrentSize() && getKey(idx).compareTo(key) == 0) {
      return getRID(idx);
    }
    return null;
  }

  public int insert(Value<?, ?> key, RID rid) {
    if (getCurrentSize() == getMaxSize()) {
      throw new RuntimeException("Leaf node is full, split??");
    }
    int position = getKeyIndex(key);
    for (int i = getCurrentSize(); i > position; i--) {
      setKey(i, getKey(i - 1));
      setRID(i, getRID(i - 1));
    }
    setKey(position, key);
    setRID(position, rid);
    increaseSize(1);
    return getCurrentSize();
  }

  public void moveHalfTo(LeafPage siblingLeaf) {
    int moveSize = getCurrentSize() - getCurrentSize() / 2;
    int startIdx = getCurrentSize() - moveSize;
    for (int i = 0; i < moveSize; i++) {
      siblingLeaf.setKey(i, getKey(i + startIdx));
      siblingLeaf.setRID(i, getRID(i + startIdx));
    }
    siblingLeaf.setCurrentSize(moveSize);
    setCurrentSize(startIdx);
  }

  public void moveAllTo(LeafPage leafLeftNode) {
    int moveStart = leafLeftNode.getCurrentSize();
    int moveSize = getCurrentSize();
    for (int i = 0; i < moveSize; i++) {
      leafLeftNode.setKey(i + moveStart, getKey(i));
      leafLeftNode.setRID(i + moveStart, getRID(i));
    }
    leafLeftNode.increaseSize(moveSize);
  }

  // remove one record in leaf node
  // returns new size
  int deleteRecord(Value<?, ?> key) {
    int position = getKeyIndex(key);
    if (position < getCurrentSize() && getKey(position).compareTo(key) == 0) {
      for (int i = position; i < getCurrentSize() - 1; i++) {
        setKey(i, getKey(i + 1));
        setRID(i, getRID(i + 1));
      }
      increaseSize(-1);
    }
    return getCurrentSize();
  }

  int deleteRecord(int idx) {
    if (idx < getCurrentSize()) {
      for (int i = idx; i < getCurrentSize() - 1; i++) {
        setKey(i, getKey(i + 1));
        setRID(i, getRID(i + 1));
      }
      increaseSize(-1);
    }
    return getCurrentSize();
  }

  // string representation
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("LeafNode(")
        .append("pageId=")
        .append(getPageId())
        .append(", parentId=")
        .append(getParentPageId())
        .append(", currentSize=")
        .append(getCurrentSize())
        .append(", maxSize=")
        .append(getMaxSize())
        .append(", nextPageId=")
        .append(getNextPageId());
    // interleave keys and rids
    for (int i = 0; i < getCurrentSize(); i++) {
      sb.append(", key");
      sb.append(i);
      sb.append("=");
      sb.append(getKey(i));
      sb.append(", rid");
      sb.append(i);
      sb.append("=");
      sb.append(getRID(i));
    }
    return sb.toString();
  }

  public StringBuilder toJson() {
    StringBuilder sb = new StringBuilder();
    sb.append("{");
    sb.append("\"pageId\":");
    sb.append(getPageId());
    sb.append(",\"pageType\":\"");
    sb.append(getPageType());
    sb.append("\",\"parentId\":");
    sb.append(getParentPageId());
    sb.append(",\"currentSize\":");
    sb.append(getCurrentSize());
    sb.append(",\"maxSize\":");
    sb.append(getMaxSize());
    sb.append(",\"nextPageId\":");
    sb.append(getNextPageId());
    // interleave keys and rids
    // json array like [{"key0":"rid0"}, {"key1":"rid1"}, ...}]
    sb.append(",\n\"pairs\":[");
    for (int i = 0; i < getCurrentSize(); i++) {
      sb.append("{\"key\":")
          .append(getKey(i))
          .append(",\"rid\":\"")
          .append(getRID(i))
          .append("\"}");
      if (i != getCurrentSize() - 1) {
        sb.append(",");
      }
    }
    sb.append("]}");
    return sb;
  }

  // print
  @Override
  public void print() {
    System.out.println(this);
  }

  public void pushFront(RID ridToBorrow, Value<?, ?> key) {
    if (getCurrentSize() == getMaxSize()) {
      throw new RuntimeException("Leaf node is full, split??");
    }
    for (int i = getCurrentSize(); i > 0; i--) {
      setKey(i, getKey(i - 1));
      setRID(i, getRID(i - 1));
    }
    setKey(0, key);
    setRID(0, ridToBorrow);
    increaseSize(1);
  }

  public void pushBack(RID ridToBorrow, Value<?, ?> key) {
    if (getCurrentSize() == getMaxSize()) {
      throw new RuntimeException("Leaf node is full, split??");
    }
    setKey(getCurrentSize(), key);
    setRID(getCurrentSize(), ridToBorrow);
    increaseSize(1);
  }
}
