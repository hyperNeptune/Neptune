package cn.edu.thssdb.storage.index;

import cn.edu.thssdb.type.Type;
import cn.edu.thssdb.type.Value;

// internal node page represents internal nodes in B+ tree
// `pointer` is page id here. It's a integer. No secrets.
// keys are values. They are serialized to bytes.
// | header | key1 | pageId1 | key2 | pageId2 | ... | keyN | pageIdN |
// because in B+ tree internal node, keys are less than pointers by one, so the first key is not
// used.
public class InternalNodePage<K extends Value<? extends Type, ?>> extends BPlusTreePage {
  public static final int ALL_HEADER_SIZE = PAGE_HEADER_SIZE + B_PLUS_TREE_PAGE_HEADER_SIZE;

  public InternalNodePage(int page_id) {
    super(page_id);
    setPageType(BTNodeType.INTERNAL);
  }
}
