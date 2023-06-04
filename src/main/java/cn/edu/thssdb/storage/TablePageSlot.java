package cn.edu.thssdb.storage;

import cn.edu.thssdb.schema.Schema;
import cn.edu.thssdb.utils.Global;
import cn.edu.thssdb.utils.Pair;
import cn.edu.thssdb.utils.RID;

import java.util.Iterator;

//
// | Page_id | Prev_page_id | Next_page_id | freeSpacePointer | tupleCount | tupleLength |
// | BitMap of allocation of tuples |
//
public class TablePageSlot extends Page implements TablePage {
  private static final int PREV_PAGE_ID_OFFSET = 0;
  private static final int NEXT_PAGE_ID_OFFSET = 4;
  private static final int FREE_SPACE_POINTER_OFFSET = 8;
  private static final int TUPLE_COUNT_OFFSET = 12;
  private static final int TUPLE_LENGTH_OFFSET = 16;
  public static final int TABLE_PAGE_SLOT_HEADER_SIZE = 20;
  public static final int ALL_HEADER_SIZE = TABLE_PAGE_SLOT_HEADER_SIZE + PAGE_HEADER_SIZE;

  public TablePageSlot(int page_id, int tupleLength) {
    super(page_id);
    setPrevPageId(Global.PAGE_ID_INVALID);
    setNextPageId(Global.PAGE_ID_INVALID);
    setFreeSpacePointer(Global.PAGE_SIZE);
    setTupleCount(0);
    setTupleLength(tupleLength);
  }

  public TablePageSlot(Page page) {
    super(page);
  }

  // setters
  public void setPrevPageId(int prev_page_id) {
    data_.putInt(PAGE_HEADER_SIZE + PREV_PAGE_ID_OFFSET, prev_page_id);
  }

  public void setNextPageId(int next_page_id) {
    data_.putInt(PAGE_HEADER_SIZE + NEXT_PAGE_ID_OFFSET, next_page_id);
  }

  public void setFreeSpacePointer(int freeSpacePointer) {
    data_.putInt(PAGE_HEADER_SIZE + FREE_SPACE_POINTER_OFFSET, freeSpacePointer);
  }

  public void setTupleCount(int tupleCount) {
    data_.putInt(PAGE_HEADER_SIZE + TUPLE_COUNT_OFFSET, tupleCount);
  }

  public void setTupleLength(int tupleLength) {
    data_.putInt(PAGE_HEADER_SIZE + TUPLE_LENGTH_OFFSET, tupleLength);
  }

  // getters
  public int getPrevPageId() {
    return data_.getInt(PAGE_HEADER_SIZE + PREV_PAGE_ID_OFFSET);
  }

  public int getNextPageId() {
    return data_.getInt(PAGE_HEADER_SIZE + NEXT_PAGE_ID_OFFSET);
  }

  public int getFreeSpacePointer() {
    return data_.getInt(PAGE_HEADER_SIZE + FREE_SPACE_POINTER_OFFSET);
  }

  public int getTupleCount() {
    return data_.getInt(PAGE_HEADER_SIZE + TUPLE_COUNT_OFFSET);
  }

  public int getTupleLength() {
    return data_.getInt(PAGE_HEADER_SIZE + TUPLE_LENGTH_OFFSET);
  }

  // remaining free space
  public int getFreeSpace() {
    return getFreeSpacePointer() - ALL_HEADER_SIZE - bitmapAreaSpace();
  }

  private boolean slotEmpty(int slotId) {
    return (data_.get(ALL_HEADER_SIZE + slotId / 8) & (1 << (slotId % 8))) == 0;
  }

  private void setSlot(int slotId) {
    data_.put(
        ALL_HEADER_SIZE + slotId / 8,
        (byte) (data_.get(ALL_HEADER_SIZE + slotId / 8) | (1 << (slotId % 8))));
  }

  private void clearSlot(int slotId) {
    data_.put(
        ALL_HEADER_SIZE + slotId / 8,
        (byte) (data_.get(ALL_HEADER_SIZE + slotId / 8) & ~(1 << slotId % 8)));
  }

  // bitmap area space
  private int bitmapAreaSpace() {
    return (slotAreaSize() + 7) / 8;
  }

  private int slotAreaSpace() {
    return Global.PAGE_SIZE - getFreeSpacePointer();
  }

  private int slotAreaSize() {
    return slotAreaSpace() / getTupleLength();
  }

  // has deleted slots, use them if no free space.
  private boolean hasDeletedSlots() {
    return getTupleCount() < slotAreaSize();
  }

  // find deleted slot id
  private int findDeletedSlot() {
    int slotId = 0;
    while (slotId < slotAreaSize()) {
      if (slotEmpty(slotId)) {
        return slotId;
      }
      slotId++;
    }
    return -1;
  }

  public enum hasEnoughSpaceResult {
    HAS_ENOUGH_SPACE,
    HAS_DELETED_SLOTS,
    NO_ENOUGH_SPACE
  }

  // check if there is enough space for a new tuple
  public hasEnoughSpaceResult hasEnoughSpace() {
    // has sufficient free space, return
    if (getFreeSpace() > getTupleLength()) {
      return hasEnoughSpaceResult.HAS_ENOUGH_SPACE;
    }
    // has deleted slots, use them if no free space.
    if (hasDeletedSlots()) {
      return hasEnoughSpaceResult.HAS_DELETED_SLOTS;
    }
    return hasEnoughSpaceResult.NO_ENOUGH_SPACE;
  }

  // insert new tuple
  // return slot id if success, -1 if failed
  public int insertTuple(Tuple tuple, RID rid) {
    // check if there is enough space for a new tuple
    hasEnoughSpaceResult result = hasEnoughSpace();
    if (result == hasEnoughSpaceResult.NO_ENOUGH_SPACE) {
      return -1;
    }
    // insert into free space
    int slotId;
    if (result == hasEnoughSpaceResult.HAS_ENOUGH_SPACE) {
      setFreeSpacePointer(getFreeSpacePointer() - getTupleLength());
      tuple.serialize(data_, getFreeSpacePointer());
      setSlot(slotId = slotAreaSize() - 1);
    } else {
      // insert into deleted slots
      slotId = findDeletedSlot();
      tuple.serialize(data_, Global.PAGE_SIZE - (slotId + 1) * getTupleLength());
      setSlot(slotId);
    }
    setTupleCount(getTupleCount() + 1);
    if (rid != null) {
      rid.setPageId(getPageId());
      rid.setSlotId(slotId);
    }
    return slotId;
  }

  // delete tuple. do not actually delete it, just mark it as deleted.
  public boolean deleteTuple(int slotId) {
    if (slotId < 0 || slotId >= slotAreaSize()) {
      return false;
    }
    if (slotEmpty(slotId)) {
      return false;
    }
    // set slot to 0
    clearSlot(slotId);
    setTupleCount(getTupleCount() - 1);
    return true;
  }

  // get tuple by slot id
  public Tuple getTuple(int slotId) {
    if (slotId < 0 || slotId >= slotAreaSize()) {
      return null;
    }
    if (slotEmpty(slotId)) {
      return null;
    }
    return Tuple.deserialize(data_, Global.PAGE_SIZE - (slotId + 1) * getTupleLength());
  }

  // update tuple by slot id
  // TODO: in update clause, we need to change tuple bitmap
  public boolean updateTuple(int slotId, Tuple tuple) {
    if (slotId < 0 || slotId >= slotAreaSize()) {
      return false;
    }
    if (slotEmpty(slotId)) {
      return false;
    }
    tuple.serialize(data_, Global.PAGE_SIZE - (slotId + 1) * getTupleLength());
    return true;
  }

  // we need tuple size
  @Override
  public void init(Object[] data) {
    setNextPageId(Global.PAGE_ID_INVALID);
    setPrevPageId(Global.PAGE_ID_INVALID);
    setFreeSpacePointer(Global.PAGE_SIZE);
    setTupleCount(0);
    setTupleLength((int) data[0]);
  }

  @Override
  public int getSlotSize() {
    return getTupleLength();
  }

  // iterator
  public class TablePageIterator implements Iterator<Pair<Tuple, Integer>> {
    private int slotId = 0;

    @Override
    public boolean hasNext() {
      while (slotId < slotAreaSize()) {
        if (!slotEmpty(slotId)) {
          return true;
        }
        slotId++;
      }
      return false;
    }

    @Override
    public Pair<Tuple, Integer> next() {
      int old_sid = slotId;
      slotId++;
      return new Pair<>(getTuple(old_sid), old_sid);
    }
  }

  // print
  public void print(Schema schema) {
    super.print();
    System.out.println("prev_page_id: " + getPrevPageId());
    System.out.println("next_page_id: " + getNextPageId());
    System.out.println("freeSpacePointer: " + getFreeSpacePointer());
    System.out.println("tupleCount: " + getTupleCount());
    System.out.println("tupleLength: " + getTupleLength());
    // print all tuples
    TablePageIterator iter = new TablePageIterator();
    while (iter.hasNext()) {
      Tuple tuple = iter.next().left;
      tuple.print(schema);
    }
  }

  // iterator
  public Iterator<Pair<Tuple, Integer>> iterator() {
    return new TablePageIterator();
  }
}
