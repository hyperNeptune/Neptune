package neptune.backend.storage.index;

import neptune.backend.buffer.BufferPoolManager;
import neptune.backend.storage.Page;
import neptune.backend.type.Value;
import neptune.common.Global;
import neptune.common.Pair;
import neptune.common.RID;

import java.io.IOException;
import java.util.Iterator;

public class BPlusTreeIterator implements Iterator<Pair<Value<?, ?>, RID>> {
  // this page is pinned upon init
  private LeafPage currPage_;
  private final BufferPoolManager bpm_;
  private int currIdx_;
  private boolean drain = false;

  public BPlusTreeIterator(LeafPage currPage, BufferPoolManager bpm, int currIdx) {
    currPage_ = currPage;
    bpm_ = bpm;
    currIdx_ = currIdx;
  }

  @Override
  public boolean hasNext() {
    if (drain) {
      return false;
    }
    // yeah, sure has next
    if (currIdx_ < currPage_.getCurrentSize()) {
      return true;
    }
    // has next page?
    if (currPage_.getNextPageId() == Global.PAGE_ID_INVALID) {
      try {
        bpm_.unpinPage(currPage_.getPageId(), false);
        drain = true;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      return false;
    }
    return true;
  }

  @Override
  public Pair<Value<?, ?>, RID> next() {
    RID rid = currPage_.getRID(currIdx_);
    Value<?, ?> key = currPage_.getKey(currIdx_);
    if (++currIdx_ >= currPage_.getCurrentSize()
        && currPage_.getNextPageId() != Global.PAGE_ID_INVALID) {
      try {
        Page nextPage = bpm_.fetchPage(currPage_.getNextPageId());
        bpm_.unpinPage(currPage_.getPageId(), false);
        currPage_ = new LeafPage(nextPage, currPage_.keyType);
        currIdx_ = 0;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    return new Pair<>(key, rid);
  }
}
