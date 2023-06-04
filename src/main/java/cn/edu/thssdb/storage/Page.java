package cn.edu.thssdb.storage;

import cn.edu.thssdb.utils.Global;

import java.nio.ByteBuffer;
import java.util.Arrays;

// this class represents a page in disk and in the database management system
// it is a byte array of size PAGE_SIZE,
// and we may need some bookkeeping information about this page
public class Page {
  protected ByteBuffer data_;
  // for mutable runtime data
  private final PageRuntimeData runtime_data_;
  public static final int PAGE_ID_OFFSET = 0;
  public static final int LSN_OFFSET = 4;
  public static final int PAGE_HEADER_SIZE = 8;

  public void resetMemory() {
    // clear means we reset the position to 0
    // rewind is useless if we don't use flip,
    // and we don't use flip because we always write whole page to disk
    data_.clear();
    Arrays.fill(data_.array(), (byte) 0);
    runtime_data_.pin_count_ = 1;
    runtime_data_.is_dirty_ = false;
  }

  public Page(int page_id) {
    // allocate will set all bytes to 0
    data_ = ByteBuffer.allocate(Global.PAGE_SIZE);
    runtime_data_ = new PageRuntimeData();
    setPageId(page_id);
  }

  // copy constructor
  // shallow copy
  public Page(Page page) {
    data_ = page.data_;
    runtime_data_ = page.runtime_data_;
  }

  public int getPageId() {
    return data_.getInt(PAGE_ID_OFFSET);
  }

  public ByteBuffer getData() {
    return data_;
  }

  public boolean isDirty() {
    return runtime_data_.is_dirty_;
  }

  public void setDirty(boolean is_dirty) {
    if (is_dirty) {
      runtime_data_.is_dirty_ = true;
    }
  }

  public int getPinCount() {
    return runtime_data_.pin_count_;
  }

  public void setPinCount(int pin_count) {
    runtime_data_.pin_count_ = pin_count;
  }

  // increase pin count by 1
  public void pin() {
    runtime_data_.pin_count_++;
  }

  // decrease pin count by 1
  public void unpin() {
    if (runtime_data_.pin_count_ > 0) {runtime_data_.pin_count_--;}
  }

  // evict able
  public boolean replaceable() {
    return runtime_data_.pin_count_ == 0;
  }

  // wrap a byte[]
  public void setData(byte[] data) {
    data_ = ByteBuffer.wrap(data);
  }

  public void setPageId(int page_id) {
    data_.putInt(PAGE_ID_OFFSET, page_id);
  }

  public int getLSN() {
    return data_.getInt(LSN_OFFSET);
  }

  public void setLSN(int lsn) {
    data_.putInt(LSN_OFFSET, lsn);
  }

  // print
  public void print() {
    // invalid page id say invalid
    if (getPageId() == Global.PAGE_ID_INVALID) {
      System.out.println("invalid page");
      return;
    }
    System.out.println("page id: " + getPageId() + ", pin count: " +
      getPinCount() + ", is dirty: " + isDirty() + "log sequence number: " + getLSN());
  }
}
