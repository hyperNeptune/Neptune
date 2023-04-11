package cn.edu.thssdb.buffer;

import cn.edu.thssdb.storage.DiskManager;
import cn.edu.thssdb.storage.Page;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BufferPoolManager {
  private final Page[] pages_;
  private final List<Integer> free_list_;
  private final DiskManager disk_manager_;
  private final Map<Integer, Integer> page_table_;
  private final ReplaceAlgorithm replacer_;

  public BufferPoolManager(int pool_size, DiskManager disk_manager, ReplaceAlgorithm replacer) {
    pages_ = new Page[pool_size];
    free_list_ = new ArrayList<>();
    disk_manager_ = disk_manager;
    page_table_ = new HashMap<>();
    replacer_ = replacer;
    for (int i = 0; i < pool_size; i++) {
      free_list_.add(i);
    }
  }

  public Page fetchPage(int page_id) throws IOException {
    if (page_table_.containsKey(page_id)) {
      int frame_id = page_table_.get(page_id);
      replacer_.recordAccess(frame_id);
      return pages_[frame_id];
    }
    if (free_list_.isEmpty()) {
      int victim_id = replacer_.getVictim();
      Page victim = pages_[victim_id];
      if (victim.isDirty()) {
        disk_manager_.writePage(victim.getPageId(), victim);
      }
      page_table_.remove(victim.getPageId());
      pages_[victim_id].resetMemory();
    }
    int frame_id = free_list_.remove(0);
    Page page = disk_manager_.readPage(page_id, pages_[frame_id]);
    pages_[frame_id] = page;
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
      pages_[frame_id].setDirty(false);
    }
  }

  public void deletePage(int page_id) throws Exception {
    throw new Exception("We are not going to implement this");
  }

  public int newPage() throws IOException {
    int page_id = disk_manager_.allocatePage();
    if (free_list_.isEmpty()) {
      int victim_id = replacer_.getVictim();
      Page victim = pages_[victim_id];
      if (victim.isDirty()) {
        disk_manager_.writePage(victim.getPageId(), victim);
      }
      page_table_.remove(victim.getPageId());
      pages_[victim_id].resetMemory();
    }
    int frame_id = free_list_.remove(0);
    Page page = disk_manager_.readPage(page_id, pages_[frame_id]);
    pages_[frame_id] = page;
    page_table_.put(page_id, frame_id);
    replacer_.recordAccess(frame_id);
    replacer_.pin(frame_id);
    return page_id;
  }

  public int getPoolSize() {
    return pages_.length;
  }
}
