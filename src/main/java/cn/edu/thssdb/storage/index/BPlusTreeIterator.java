package cn.edu.thssdb.storage.index;

import cn.edu.thssdb.buffer.BufferPoolManager;
import cn.edu.thssdb.storage.Page;
import cn.edu.thssdb.utils.Global;
import cn.edu.thssdb.utils.RID;

import java.io.IOException;
import java.util.Iterator;

public class BPlusTreeIterator implements Iterator<RID> {
  private LeafPage currPage_;
  private final BufferPoolManager bpm_;
  private int currIdx_;

  public BPlusTreeIterator(LeafPage currPage, BufferPoolManager bpm, int currIdx) {
    currPage_ = currPage;
    bpm_ = bpm;
    currIdx_ = currIdx;
  }

  @Override
  public boolean hasNext() {
    // yeah, sure has next
    if (currIdx_ < currPage_.getCurrentSize()) {
      return true;
    }
    // has next page?
    return currPage_.getNextPageId() != Global.PAGE_ID_INVALID;
  }

  @Override
  public RID next() {
    RID rid = currPage_.getRID(currIdx_);
    if (++currIdx_ >= currPage_.getCurrentSize() && currPage_.getNextPageId() != Global.PAGE_ID_INVALID) {
      try {
        bpm_.unpinPage(currPage_.getPageId(), false);
        Page nextPage = bpm_.fetchPage(currPage_.getNextPageId());
        currPage_ = new LeafPage(nextPage, currPage_.keyType);
        currIdx_ = 0;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    return rid;
  }
}
