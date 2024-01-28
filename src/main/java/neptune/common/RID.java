package neptune.common;

public class RID implements java.io.Serializable {
  private int pageId_ = -1;
  private int slotId_ = -1;

  public RID() {}

  public RID(int pageId, int slotId) {
    pageId_ = pageId;
    slotId_ = slotId;
  }

  public void setPageId(int pageId) {
    pageId_ = pageId;
  }

  public void setSlotId(int slotId) {
    slotId_ = slotId;
  }

  public int getPageId() {
    return pageId_;
  }

  public int getSlotId() {
    return slotId_;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (obj == this) {
      return true;
    }
    if (obj.getClass() != getClass()) {
      return false;
    }
    RID rid = (RID) obj;
    return pageId_ == rid.pageId_ && slotId_ == rid.slotId_;
  }

  @Override
  public int hashCode() {
    return pageId_ * 31 + slotId_;
  }

  @Override
  public String toString() {
    return String.format("RID(%d, %d)", pageId_, slotId_);
  }

  // copy assign
  public void assign(RID rid) {
    pageId_ = rid.pageId_;
    slotId_ = rid.slotId_;
  }

  // get size
  public static int getSize() {
    return 2 * Integer.BYTES;
  }
}
