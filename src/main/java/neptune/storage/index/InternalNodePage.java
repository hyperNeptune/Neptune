package neptune.storage.index;

import neptune.buffer.BufferPoolManager;
import neptune.storage.Page;
import neptune.type.Type;
import neptune.type.Value;
import neptune.utils.Global;

import java.io.IOException;

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
  // NOTE: did not bypass the unused first key
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

  public void moveAllTo(InternalNodePage siblingInternal, Value<?, ?> fallenKey) {
    int moveSize = getCurrentSize();
    int start = siblingInternal.getCurrentSize();
    siblingInternal.setKey(start, fallenKey);
    siblingInternal.setPointer(start, getPointer(0));
    for (int i = 1; i < moveSize; i++) {
      siblingInternal.setKey(i + start, getKey(i));
      siblingInternal.setPointer(i + start, getPointer(i));
    }
    siblingInternal.increaseSize(moveSize);
  }

  private void insertAfter(int pageId, Value<?, ?> key, int pageId1, boolean front) {
    // find pageId
    int index = front ? -1 : findPointerIndex(pageId);
    // move
    for (int i = getCurrentSize(); i > index + 1; i--) {
      setKey(i, getKey(i - 1));
      setPointer(i, getPointer(i - 1));
    }
    // insert
    if (front) {
      index++;
    }
    setKey(index + 1, key);
    if (!front) {
      setPointer(index + 1, pageId1);
    }
    setCurrentSize(getCurrentSize() + 1);
  }

  // insert <key, pageId1> after pageId
  public void insertAfter(int pageId, Value<?, ?> key, int pageId1) {
    insertAfter(pageId, key, pageId1, false);
  }

  // insert <pointer, key> to the front of the page
  public void pushFront(int pointer, Value<?, ?> key) {
    // move all elements to its next position
    insertAfter(0, key, pointer, true);
    // set pointer NOTE the first key is unused.
    setPointer(0, pointer);
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

  public StringBuilder toJson(BufferPoolManager bpm) throws IOException {
    StringBuilder sb = new StringBuilder();
    sb.append("{")
        .append("\"pageId\":")
        .append(getPageId())
        .append(",")
        .append("\"pageType\":\"")
        .append(getPageType())
        .append("\",")
        .append("\"parentPageId\":")
        .append(getParentPageId())
        .append(",")
        .append("\"currentSize\":")
        .append(getCurrentSize())
        .append(",")
        .append("\"maxSize\":")
        .append(getMaxSize())
        .append(",")
        .append("\"keyType\":\"")
        .append(keyType)
        .append("\",");
    // interleave keys and pointers, the first key is not used, but pointer is.
    // It's like [pointer0: (page), key1: (page), pointer1: (page), key2: (page), ...]
    sb.append("\"keys and pointers\": {");
    sb.append("\"page")
        .append(getPointer(0))
        .append("\":")
        .append(new BPlusTreePage(bpm.fetchPage(getPointer(0)), keyType).toJsonBPTP(bpm))
        .append(",");
    bpm.unpinPage(getPointer(0), false);
    for (int i = 1; i < getCurrentSize(); i++) {
      sb.append("\"key")
          .append(getKey(i))
          .append("\":")
          .append(getKey(i))
          .append(",")
          .append("\"page")
          .append(getPointer(i))
          .append("\":")
          .append(new BPlusTreePage(bpm.fetchPage(getPointer(i)), keyType).toJsonBPTP(bpm));
      bpm.unpinPage(getPointer(i), false);
      if (i < getCurrentSize() - 1) {
        sb.append(",");
      }
    }
    sb.append("}}");
    return sb;
  }

  // print
  @Override
  public void print() {
    System.out.println(this);
  }

  public int findPointerIndex(int pageId) {
    int index = 0;
    for (; index < getCurrentSize(); index++) {
      if (getPointer(index) == pageId) {
        break;
      }
    }
    return index;
  }

  int deleteRecord(Value<?, ?> key) {
    int position = getKeyIndex(key);
    if (position < getCurrentSize() && getKey(position).compareTo(key) == 0) {
      for (int i = position; i < getCurrentSize() - 1; i++) {
        setKey(i, getKey(i + 1));
        setPointer(i, getPointer(i + 1));
      }
      increaseSize(-1);
    }
    return getCurrentSize();
  }

  int deleteRecord(int index) {
    if (index < getCurrentSize()) {
      for (int i = index; i < getCurrentSize() - 1; i++) {
        setKey(i, getKey(i + 1));
        setPointer(i, getPointer(i + 1));
      }
      increaseSize(-1);
    }
    return getCurrentSize();
  }

  public void pushBack(int pointerToBorrow, Value<?, ?> key) {
    setKey(getCurrentSize(), key);
    setPointer(getCurrentSize(), pointerToBorrow);
    increaseSize(1);
  }
}
