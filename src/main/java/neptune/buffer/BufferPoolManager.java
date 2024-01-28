package neptune.buffer;

import neptune.storage.DiskManager;
import neptune.storage.Page;
import neptune.utils.Global;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

public class BufferPoolManager {
  private final Page[] pages_;
  private final List<Integer> free_list_;
  private final DiskManager disk_manager_;
  private final Map<Integer, Integer> page_table_; // <page_id, frame_id>
  private final ReplaceAlgorithm replacer_;
  // latch
  private final ReentrantLock latch_ = new ReentrantLock(true);

  public BufferPoolManager(int pool_size, DiskManager disk_manager, ReplaceAlgorithm replacer) {
    pages_ = new Page[pool_size];
    free_list_ = new ArrayList<>();
    disk_manager_ = disk_manager;
    page_table_ = new ConcurrentHashMap<>();
    replacer_ = replacer;
    for (int i = 0; i < pool_size; i++) {
      free_list_.add(i);
      // Java array doesn't init its elements...
      pages_[i] = new Page(Global.PAGE_ID_INVALID);
    }
  }

  public Page fetchPage(int page_id) throws IOException {
    Integer frameId;
    Page page;

    latch_.lock();
    if (page_table_.containsKey(page_id)) {
      frameId = page_table_.get(page_id);
      page = pages_[frameId];
      page.pin();
    } else {
      if ((frameId = findAFreePageFrame()) == null) {
        latch_.unlock();
        return null;
      }
      pages_[frameId].getData().clear();
      page = disk_manager_.readPage(page_id, pages_[frameId]);
      page.getData().clear();
      page.setPageId(page_id);
      page_table_.put(page_id, frameId);
    }

    replacer_.recordAccess(frameId);
    replacer_.pin(frameId);
    latch_.unlock();
    return page;
  }

  public void unpinPage(int page_id, boolean is_dirty) throws IOException {
    // page_table_ is thread safe... maybe?
    if (!page_table_.containsKey(page_id)) {
      return;
    }

    latch_.lock();
    int frame_id = page_table_.get(page_id);
    pages_[frame_id].unpin();
    if (pages_[frame_id].replaceable()) {
      replacer_.unpin(frame_id);
    }
    pages_[frame_id].setDirty(is_dirty | pages_[frame_id].isDirty());
    latch_.unlock();
  }

  public void flushAllPages() throws IOException {
    latch_.lock();
    for (Page page : pages_) {
      if (page != null && page.getPageId() != Global.PAGE_ID_INVALID) {
        disk_manager_.writePage(page.getPageId(), page);
        page.getData().clear();
        page.setDirty(false);
      }
    }
    latch_.unlock();
  }

  // flushPage
  public void flushPage(int page_id) throws IOException {
    if (!page_table_.containsKey(page_id)) {
      return;
    }
    latch_.lock();
    int frame_id = page_table_.get(page_id);
    disk_manager_.writePage(pages_[frame_id].getPageId(), pages_[frame_id]);
    pages_[frame_id].getData().clear();
    pages_[frame_id].setDirty(false);
    latch_.unlock();
  }

  public void deletePage(int page_id) throws Exception {
    throw new Exception("We are not going to implement this");
  }

  // ATTENTION: this method is not thread-safe
  // need latch protecting
  private Integer findAFreePageFrame() throws IOException {
    int frame_id;
    if (!free_list_.isEmpty()) {
      frame_id = free_list_.remove(0);
    } else if ((frame_id = replacer_.getVictim()) == -1) {
      return null;
    } else if (pages_[frame_id].isDirty()) {
      // write back
      disk_manager_.writePage(pages_[frame_id].getPageId(), pages_[frame_id]);
      pages_[frame_id].getData().clear();
    }
    page_table_.remove(pages_[frame_id].getPageId());
    pages_[frame_id].resetMemory();
    return frame_id;
  }

  public Page newPage() throws IOException {
    Integer frame_id;
    latch_.lock();
    if ((frame_id = findAFreePageFrame()) == null) {
      latch_.unlock();
      return null;
    }

    // start allocate this page
    int page_id = disk_manager_.allocatePage();
    Page page = pages_[frame_id];
    page.setPageId(page_id);

    page_table_.put(page_id, frame_id);
    replacer_.recordAccess(frame_id);
    replacer_.pin(frame_id);
    latch_.unlock();
    return page;
  }

  public int getPoolSize() {
    latch_.lock();
    int size_local = replacer_.size();
    latch_.unlock();
    return size_local;
  }
}
