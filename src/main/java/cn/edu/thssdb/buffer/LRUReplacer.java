package cn.edu.thssdb.buffer;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

public class LRUReplacer implements ReplaceAlgorithm {
  int size_;
  // use a queue
  // when a page is accessed, move it to the end of the queue
  Queue<Integer> page_list_;
  // true if the page is unpinned
  Map<Integer, Boolean> evict_map_;

  public LRUReplacer(int pool_size) {
    size_ = pool_size;
    page_list_ = new LinkedList<>();
    evict_map_ = new HashMap<>();
  }

  @Override
  public int getVictim() {
    // iterate the queue to find the first page that is not pinned
    // if all pages are pinned, return -1
    int victim = -1;
    for (int page_id : page_list_) {
      if (evict_map_.get(page_id)) {
        victim = page_id;
        break;
      }
    }
    if (victim != -1) {
      page_list_.remove(victim);
      evict_map_.remove(victim);
    }
    return victim;
  }

  @Override
  public void recordAccess(int page_id) {
    page_list_.remove(page_id);
    page_list_.add(page_id);
    pin(page_id);
  }

  @Override
  public void unpin(int page_id) {
    evict_map_.put(page_id, true);
  }

  @Override
  public void pin(int page_id) {
    evict_map_.put(page_id, false);
  }

  @Override
  public int size() {
    return page_list_.size();
  }
}
