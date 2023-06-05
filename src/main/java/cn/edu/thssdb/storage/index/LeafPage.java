package cn.edu.thssdb.storage.index;

// leaf node page represents leaf nodes in B+ tree
// we are late materialized here. So leaf node stores `<key, RID>`
// it's like:
// | header | nextPageId | key1 | RID1 | key2 | RID2 | ... | keyN | RIDN |
public class LeafPage extends BPlusTreePage {
  public static final int NEXT_PAGE_ID_OFFSET = 0;
  public static final int LEAF_NODE_HEADER_SIZE = 4;
  public static final int ALL_PAGE_HEADER_SIZE =
      PAGE_HEADER_SIZE + B_PLUS_TREE_PAGE_HEADER_SIZE + LEAF_NODE_HEADER_SIZE;

  public LeafPage(int page_id) {
    super(page_id);
    setPageType(BTNodeType.LEAF);
  }

  // getters and setters
  public int getNextPageId() {
    return data_.getInt(NEXT_PAGE_ID_OFFSET);
  }

  public void setNextPageId(int next_page_id) {
    data_.putInt(NEXT_PAGE_ID_OFFSET, next_page_id);
  }
}
