package cn.edu.thssdb.storage;

import cn.edu.thssdb.utils.Global;

import java.nio.ByteBuffer;
import java.util.Arrays;

// this class represents a page in disk and in the database management system
// it is a byte array of size PAGE_SIZE,
// and we may need some bookkeeping information about this page
public class Page {
  protected int page_id_;
  protected ByteBuffer data_;
  private boolean is_dirty_;
  private int pin_count_;
  private static final int PAGE_ID_OFFSET = 0;

  public void resetMemory() {
    // clear means we reset the position to 0
    // rewind is useless if we don't use flip,
    // and we don't use flip because we always write whole page to disk
    data_.clear();
    Arrays.fill(data_.array(), (byte) 0);
    pin_count_ = 1;
    is_dirty_ = false;
  }

  public Page(int page_id) {
    page_id_ = page_id;
    // allocate will set all bytes to 0
    data_ = ByteBuffer.allocate(Global.PAGE_SIZE);
    setPageId(page_id_);
  }

  public int getPageId() {
    return page_id_;
  }

  public ByteBuffer getData() {
    return data_;
  }

  public boolean isDirty() {
    return is_dirty_;
  }

  public void setDirty(boolean is_dirty) {
    if (is_dirty) is_dirty_ = true;
  }

  public int getPinCount() {
    return pin_count_;
  }

  public void setPinCount(int pin_count) {
    pin_count_ = pin_count;
  }

  // increase pin count by 1
  public void pin() {
    pin_count_++;
  }

  // decrease pin count by 1
  public void unpin() {
    pin_count_--;
  }

  // evict able
  public boolean replaceable() {
    return pin_count_ == 0;
  }

  // wrap a byte[]
  public void setData(byte[] data) {
    data_ = ByteBuffer.wrap(data);
  }

  public void setPageId(int page_id) {
    page_id_ = page_id;
    data_.putInt(PAGE_ID_OFFSET, page_id);
  }

  // print
  public void print() {
    // invalid pageid say invalid
    if (page_id_ == -1) {
      System.out.println("invalid page");
      return;
    }
    System.out.println("page id: " + page_id_);
  }
}
