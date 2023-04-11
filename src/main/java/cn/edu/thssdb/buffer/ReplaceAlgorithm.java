package cn.edu.thssdb.buffer;

public interface ReplaceAlgorithm {
  int getVictim();

  void recordAccess(int page_id);

  void unpin(int page_id);

  void pin(int page_id);

  int size();
}
