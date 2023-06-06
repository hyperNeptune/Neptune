package cn.edu.thssdb.storage.index;

import cn.edu.thssdb.storage.Page;
import cn.edu.thssdb.type.Type;
import cn.edu.thssdb.type.Value;
import cn.edu.thssdb.utils.Global;

// internal node page represents internal nodes in B+ tree
// `pointer` is page id here. It's an integer. No secrets.
// keys are values. They are serialized to bytes.
// | header | key1 | pageId1 | key2 | pageId2 | ... | keyN | pageIdN |
// because in B+ tree internal node, keys are less than pointers by one, so the first key is not
// used.
public class InternalNodePage extends BPlusTreePage {
  public static final int ALL_HEADER_SIZE = PAGE_HEADER_SIZE + B_PLUS_TREE_PAGE_HEADER_SIZE;

  // convert a blank page to a internal node page
  public InternalNodePage(Page page, Type keyType) {
    super(page, keyType);
    data_ = page.getData();
  }

  // convert a B plus tree page to a internal node page
  public InternalNodePage(BPlusTreePage page, Type keyType) {
    super(page, keyType);
  }

  // init (it's better to init separately)
  public void init(int parentId) {
    setPageType(BTNodeType.INTERNAL);
    setParentPageId(parentId);
    setCurrentSize(0);
    setMaxSize((Global.PAGE_SIZE - ALL_HEADER_SIZE) / (keyType.getTypeSize() + Integer.BYTES));
  }

  public void init(int parentId, int maxSize) {
    init(parentId);
    if (maxSize != BPlusTree.MAXSIZE_DECIDE_BY_PAGE) {
      setMaxSize(maxSize);
    }
  }

  // getters and setters
  @Override
  public Value<?, ?> getKey(int index) {
    if (index >= getCurrentSize()) {
      return null;
    }
    int offset = ALL_HEADER_SIZE + index * (keyType.getTypeSize() + Integer.BYTES);
    return keyType.deserializeValue(data_, offset);
  }

  public int getPointer(int index) {
    if (index >= getCurrentSize()) {
      return Global.PAGE_ID_INVALID;
    }
    int offset =
        ALL_HEADER_SIZE + index * (keyType.getTypeSize() + Integer.BYTES) + keyType.getTypeSize();
    return data_.getInt(offset);
  }

  int lookUp(Value<?, ?> keyValue) {
    for (int i = 1; i < getCurrentSize(); i++) {
      if (keyValue.compareTo(getKey(i)) < 0) {
        return getPointer(i - 1);
      }
    }
    return getPointer(getCurrentSize() - 1);
  }

  public void setKey(int index, Value<?, ?> key) {
    if (index >= getMaxSize()) {
      return;
    }
    int offset = ALL_HEADER_SIZE + index * (keyType.getTypeSize() + Integer.BYTES);
    key.serialize(data_, offset);
  }

  public void setPointer(int index, int pageId) {
    if (index >= getMaxSize()) {
      return;
    }
    int offset =
        ALL_HEADER_SIZE + index * (keyType.getTypeSize() + Integer.BYTES) + keyType.getTypeSize();
    data_.putInt(offset, pageId);
  }

  public void moveHalfTo(InternalNodePage siblingInternal) {
    int moveSize = getCurrentSize() - getCurrentSize() / 2;
    int start = getCurrentSize() - moveSize;
    for (int i = 0; i < moveSize; i++) {
      siblingInternal.setKey(i, getKey(i + start));
      siblingInternal.setPointer(i, getPointer(i + start));
    }
    setCurrentSize(start);
    siblingInternal.setCurrentSize(moveSize);
  }

  public void insertAfter(int pageId, Value<?, ?> key, int pageId1) {
    // find pageId
    int index = 0;
    for (; index < getCurrentSize(); index++) {
      if (getPointer(index) == pageId) {
        break;
      }
    }
    // move
    for (int i = getCurrentSize(); i > index + 1; i--) {
      setKey(i, getKey(i - 1));
      setPointer(i, getPointer(i - 1));
    }
    // insert
    setKey(index + 1, key);
    setPointer(index + 1, pageId1);
    setCurrentSize(getCurrentSize() + 1);
  }

  // to string
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("InternalNodePage: ");
    sb.append("pageId: ").append(getPageId()).append(", ");
    sb.append("pageType: ").append(getPageType()).append(", ");
    sb.append("parentPageId: ").append(getParentPageId()).append(", ");
    sb.append("currentSize: ").append(getCurrentSize()).append(", ");
    sb.append("maxSize: ").append(getMaxSize()).append(", ");
    sb.append("keyType: ").append(keyType).append(", ");
    // interleave keys and pointers, the first key is not used, but pointer is
    sb.append("keys and pointers: ");
    sb.append("pageId").append(0).append(": ").append(getPointer(0)).append(", ");
    for (int i = 1; i < getCurrentSize(); i++) {
      sb.append("key").append(i).append(": ").append(getKey(i)).append(", ");
      sb.append("pageId").append(i).append(": ").append(getPointer(i)).append(", ");
    }
    return sb.toString();
  }

  // print
  @Override
  public void print() {
    System.out.println(this);
  }
}
