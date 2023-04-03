package cn.edu.thssdb.storage;

import cn.edu.thssdb.utils.Global;

import java.nio.ByteBuffer;

// this class represents a page in disk and in the database management system
// it is a byte array of size PAGE_SIZE
// and we may need some bookkeeping information about this page
public class Page {
  private int page_id_;
  private ByteBuffer data_;

  private void resetMemory() {
    data_.clear();
  }

  public Page(int page_id) {
    page_id_ = page_id;
    data_ = ByteBuffer.allocate(Global.PAGE_SIZE);
    resetMemory();
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
}
