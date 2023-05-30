package cn.edu.thssdb.buffer;

import cn.edu.thssdb.storage.DiskManager;
import cn.edu.thssdb.storage.Page;
import cn.edu.thssdb.utils.Global;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
    page_table_ = new HashMap<>();
    replacer_ = replacer;
    for (int i = 0; i < pool_size; i++) {
      free_list_.add(i);
      // Java array doesn't init its elements...
      pages_[i] = new Page(Global.PAGE_ID_INVALID);
    }
  }

  public Page fetchPage(int page_id) throws IOException {
    if (page_table_.containsKey(page_id)) {
      int frame_id = page_table_.get(page_id);
      replacer_.recordAccess(frame_id);
      return pages_[frame_id];
    }
    // find proper frame to store the page
    int frame_id;
    if (free_list_.isEmpty()) {
      int victim_id = replacer_.getVictim();
      if (victim_id == -1) {
        return null;
      }
      Page victim = pages_[victim_id];
      if (victim.isDirty()) {
        disk_manager_.writePage(victim.getPageId(), victim);
        victim.getData().clear();
      }
      page_table_.remove(victim.getPageId());
      frame_id = victim_id;
    } else {
      frame_id = free_list_.remove(0);
    }

    pages_[frame_id].getData().clear();
    Page page = disk_manager_.readPage(page_id, pages_[frame_id]);
    page.getData().clear();
    pages_[frame_id] = page;
    page.setPageId(page_id);

    page_table_.put(page_id, frame_id);

    replacer_.recordAccess(frame_id);
    replacer_.pin(frame_id);
    return page;
  }

  public void unpinPage(int page_id, boolean is_dirty) throws IOException {
    if (!page_table_.containsKey(page_id)) {
      return;
    }
    int frame_id = page_table_.get(page_id);
    pages_[frame_id].unpin();
    if (pages_[frame_id].replaceable()) replacer_.unpin(frame_id);
    pages_[frame_id].setDirty(is_dirty);
  }

  public void flushAllPages() throws IOException {
    for (Page page : pages_) {
      if (page != null && page.isDirty()) {
        disk_manager_.writePage(page.getPageId(), page);
        page.getData().clear();
        page.setDirty(false);
      }
    }
  }

  // flushPage
  public void flushPage(int page_id) throws IOException {
    if (!page_table_.containsKey(page_id)) {
      return;
    }
    int frame_id = page_table_.get(page_id);
    if (pages_[frame_id].isDirty()) {
      disk_manager_.writePage(pages_[frame_id].getPageId(), pages_[frame_id]);
      pages_[frame_id].getData().clear();
      pages_[frame_id].setDirty(false);
    }
  }

  public void deletePage(int page_id) throws Exception {
    throw new Exception("We are not going to implement this");
  }

  public Page newPage() throws IOException {
    int page_id = disk_manager_.allocatePage();
    int frame_id;
    if (free_list_.isEmpty()) {
      int victim_id = replacer_.getVictim();
      if (victim_id == -1) {
        return null;
      }
      Page victim = pages_[victim_id];
      if (victim.isDirty()) {
        disk_manager_.writePage(victim.getPageId(), victim);
      }
      page_table_.remove(victim.getPageId());
      frame_id = victim_id;
    } else {
      frame_id = free_list_.remove(0);
    }

    Page page = pages_[frame_id];
    pages_[frame_id].resetMemory();
    page.setPageId(page_id);

    page_table_.put(page_id, frame_id);
    replacer_.recordAccess(frame_id);
    replacer_.pin(frame_id);
    return page;
  }

  public int getPoolSize() {
    return replacer_.size();
  }
}
