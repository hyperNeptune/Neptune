package neptune.storage.index;

import neptune.buffer.BufferPoolManager;
import neptune.storage.Page;
import neptune.type.Type;
import neptune.type.Value;
import neptune.utils.Global;

import java.io.IOException;

// shared header for B+ tree pages
// | page header (Page ID, LSN)| pageType | currentSize(number of pairs) |
// | maxSize (max number of pairs) | parentPageId|
public class BPlusTreePage extends Page {
  public enum BTNodeType {
    LEAF,
    INTERNAL
  } // 4 bytes

  public static final int PAGE_TYPE_OFFSET = 0;
  public static final int CURRENT_SIZE_OFFSET = 4;
  public static final int MAX_SIZE_OFFSET = 8;
  public static final int PARENT_PAGE_ID_OFFSET = 12;
  public static final int B_PLUS_TREE_PAGE_HEADER_SIZE = 16;
  protected final Type keyType;

  // cast a page to a B+ tree page
  public BPlusTreePage(Page page, Type keyType) {
    super(page);
    this.keyType = keyType;
  }

  // getter and setters
  public BTNodeType getPageType() {
    return data_.getInt(PAGE_HEADER_SIZE + PAGE_TYPE_OFFSET) == 0
        ? BTNodeType.LEAF
        : BTNodeType.INTERNAL;
  }

  public void setPageType(BTNodeType page_type) {
    data_.putInt(PAGE_HEADER_SIZE + PAGE_TYPE_OFFSET, page_type == BTNodeType.LEAF ? 0 : 1);
  }

  public int getCurrentSize() {
    return data_.getInt(PAGE_HEADER_SIZE + CURRENT_SIZE_OFFSET);
  }

  public void setCurrentSize(int current_size) {
    data_.putInt(PAGE_HEADER_SIZE + CURRENT_SIZE_OFFSET, current_size);
  }

  public int getMaxSize() {
    return data_.getInt(PAGE_HEADER_SIZE + MAX_SIZE_OFFSET);
  }

  public void setMaxSize(int max_size) {
    data_.putInt(PAGE_HEADER_SIZE + MAX_SIZE_OFFSET, max_size);
  }

  public int getParentPageId() {
    return data_.getInt(PAGE_HEADER_SIZE + PARENT_PAGE_ID_OFFSET);
  }

  public void setParentPageId(int parent_page_id) {
    data_.putInt(PAGE_HEADER_SIZE + PARENT_PAGE_ID_OFFSET, parent_page_id);
  }

  public boolean isRootPage() {
    return getParentPageId() == Global.PAGE_ID_INVALID;
  }

  public boolean isFull() {
    return getCurrentSize() == getMaxSize();
  }

  public boolean isUnderflow() {
    return getCurrentSize() < getMaxSize() / 2;
  }

  public boolean increaseSize(int amount) {
    if (getCurrentSize() + amount > getMaxSize()) {
      return false;
    }
    setCurrentSize(getCurrentSize() + amount);
    return true;
  }

  int getKeyIndex(Value<?, ?> v) {
    int left = 0;
    int right = getCurrentSize();
    if (left >= right) {
      return right;
    }
    while (left < right) {
      int mid = (left + right) / 2;
      Value<?, ?> mid_key = getKey(mid);
      if (mid_key == null) {
        throw new RuntimeException("mid_key is null, impossible??");
      }
      if (mid_key.compareTo(v) < 0) {
        left = mid + 1;
      } else {
        right = mid;
      }
    }
    return left;
  }

  Value<?, ?> getKey(int mid) {
    return null;
  }

  public StringBuilder toJsonBPTP(BufferPoolManager bpm) throws IOException {
    if (getPageType() == BTNodeType.LEAF) {
      return new LeafPage(this, keyType).toJson();
    } else {
      return new InternalNodePage(this, keyType).toJson(bpm);
    }
  }
}
