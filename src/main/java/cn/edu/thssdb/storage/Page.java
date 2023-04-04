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

  private static final int PAGE_ID_OFFSET = 0;

  public void resetMemory() {
    // clear means we reset the position to 0
    // rewind is useless if we don't use flip,
    // and we don't use flip because we always write whole page to disk
    data_.clear();
    Arrays.fill(data_.array(), (byte) 0);
  }

  public Page(int page_id) {
    page_id_ = page_id;
    // allocate will set all bytes to 0
    data_ = ByteBuffer.allocate(Global.PAGE_SIZE);
    SetPageId(page_id_);
  }

  public int GetPageId() {
    return page_id_;
  }

  public ByteBuffer GetData() {
    return data_;
  }

  // wrap a byte[]
  public void SetData(byte[] data) {
    data_ = ByteBuffer.wrap(data);
  }

  public void SetPageId(int page_id) {
    page_id_ = page_id;
    data_.putInt(PAGE_ID_OFFSET, page_id);
  }
}
