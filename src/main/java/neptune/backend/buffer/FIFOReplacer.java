package neptune.backend.buffer;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

public class FIFOReplacer implements ReplaceAlgorithm {
  int size_;
  Queue<Integer> page_list_;
  Map<Integer, Boolean> evict_map_;

  public FIFOReplacer(int pool_size) {
    size_ = pool_size;
    page_list_ = new ConcurrentLinkedQueue<>();
    evict_map_ = new ConcurrentHashMap<>();
  }

  @Override
  public int getVictim() {
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
    // if not exists in evict_map_, add it
    if (!evict_map_.containsKey(page_id)) {
      evict_map_.put(page_id, false);
      page_list_.add(page_id);
    }
    pin(page_id);
  }

  @Override
  public void unpin(int page_id) {
    evict_map_.put(page_id, false);
  }

  @Override
  public void pin(int page_id) {
    evict_map_.put(page_id, true);
  }

  @Override
  public int size() {
    return page_list_.size();
  }
}
