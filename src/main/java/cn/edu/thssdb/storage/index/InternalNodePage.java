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
    data_ = page.getData();
  }

  // init (it's better to init separately)
  public void init(int parentId) {
    setPageType(BTNodeType.INTERNAL);
    setParentPageId(parentId);
    setCurrentSize(0);
    setMaxSize((Global.PAGE_SIZE - ALL_HEADER_SIZE) / (keyType.getTypeSize() + Integer.BYTES));
  }

  // getters and setters
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
}
