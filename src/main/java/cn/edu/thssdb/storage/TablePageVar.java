package cn.edu.thssdb.storage;

import cn.edu.thssdb.utils.Global;
import cn.edu.thssdb.utils.Pair;
import cn.edu.thssdb.utils.RID;

import java.util.Iterator;

//
// | Page_id | Prev_page_id | Next_page_id | freeSpacePointer | slotCount | tupleCount |
// | meta info about all Tuples: (Tuple offset, Tuple size, Tuple flags) |
// for now, tuple flags are all about whether the tuple is allocated or not
//
public class TablePageVar extends Page implements TablePage {
  private static final int PREV_PAGE_ID_OFFSET = 0;
  private static final int NEXT_PAGE_ID_OFFSET = 4;
  private static final int FREE_SPACE_POINTER_OFFSET = 8;
  private static final int SLOT_COUNT_OFFSET = 12;
  private static final int TUPLE_COUNT_OFFSET = 16;
  public static final int TABLE_PAGE_VAR_HEADER_SIZE = 20;
  public static final int ALL_HEADER_SIZE = TABLE_PAGE_VAR_HEADER_SIZE + PAGE_HEADER_SIZE;
  // dynamic part
  public static final int TUPLE_META_SIZE = 12;
  public static final int TUPLE_OFFSET_OFFSET = 0;
  public static final int TUPLE_SIZE_OFFSET = 4;
  public static final int TUPLE_FLAGS_OFFSET = 8;
  public static final int SLOT_DELETED = 0;
  public static final int SLOT_ALLOCATED = 1;

  public TablePageVar(int page_id) {
    super(page_id);
    setPrevPageId(Global.PAGE_ID_INVALID);
    setNextPageId(Global.PAGE_ID_INVALID);
    setFreeSpacePointer(Global.PAGE_SIZE);
    setSlotCount(0);
    setTupleCount(0);
  }

  public TablePageVar(Page page) {
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

  public void setSlotCount(int slotCount) {
    data_.putInt(PAGE_HEADER_SIZE + SLOT_COUNT_OFFSET, slotCount);
  }

  public void setTupleCount(int tupleCount) {
    data_.putInt(PAGE_HEADER_SIZE + TUPLE_COUNT_OFFSET, tupleCount);
  }

  public void setTupleLength(int slotId, int size) {
    data_.putInt(ALL_HEADER_SIZE + slotId * TUPLE_META_SIZE + TUPLE_SIZE_OFFSET, size);
  }

  public void setTupleOffset(int slotId, int offset) {
    data_.putInt(ALL_HEADER_SIZE + slotId * TUPLE_META_SIZE + TUPLE_OFFSET_OFFSET, offset);
  }

  public void setTupleFlags(int slotId, int flags) {
    data_.putInt(ALL_HEADER_SIZE + slotId * TUPLE_META_SIZE + TUPLE_FLAGS_OFFSET, flags);
  }

  // mark delete
  public void markDelete(int slotId) {
    setTupleFlags(slotId, SLOT_DELETED);
  }

  // mark allocated
  public void markAllocated(int slotId) {
    setTupleFlags(slotId, SLOT_ALLOCATED);
  }

  public void setTupleMeta(int slotId, int offset, int size, int flags) {
    setTupleOffset(slotId, offset);
    setTupleLength(slotId, size);
    setTupleFlags(slotId, flags);
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

  public int getSlotCount() {
    return data_.getInt(PAGE_HEADER_SIZE + SLOT_COUNT_OFFSET);
  }

  public int getTupleCount() {
    return data_.getInt(PAGE_HEADER_SIZE + TUPLE_COUNT_OFFSET);
  }

  public int getTupleLength(int slotId) {
    return data_.getInt(ALL_HEADER_SIZE + slotId * TUPLE_META_SIZE + TUPLE_SIZE_OFFSET);
  }

  public int getTupleOffset(int slotId) {
    return data_.getInt(ALL_HEADER_SIZE + slotId * TUPLE_META_SIZE + TUPLE_OFFSET_OFFSET);
  }

  public int getTupleFlags(int slotId) {
    return data_.getInt(ALL_HEADER_SIZE + slotId * TUPLE_META_SIZE + TUPLE_FLAGS_OFFSET);
  }

  // tuple is allocated
  public boolean isAllocated(int slotId) {
    return (getTupleFlags(slotId) & SLOT_ALLOCATED) == 1;
  }

  // remaining free space
  public int getFreeSpace() {
    return getFreeSpacePointer() - ALL_HEADER_SIZE - MetaAreaSpace();
  }

  // meta info area space
  private int MetaAreaSpace() {
    return getSlotCount() * TUPLE_META_SIZE;
  }

  private int slotAreaSpace() {
    return Global.PAGE_SIZE - getFreeSpacePointer();
  }

  // has deleted slots, use them if no free space.
  private boolean hasDeletedSlots() {
    return getTupleCount() != getSlotCount();
  }

  // find deleted slot id
  private int findDeletedSlot(int slotSize) {
    int slotId = 0;
    while (slotId < getTupleCount()) {
      if (!isAllocated(slotId) && getTupleLength(slotId) >= slotSize) {
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
  public hasEnoughSpaceResult hasEnoughSpace(int size) {
    // has sufficient free space, return
    if (getFreeSpace() > size + TUPLE_META_SIZE) {
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
    hasEnoughSpaceResult result = hasEnoughSpace(tuple.getSize());
    if (result == hasEnoughSpaceResult.NO_ENOUGH_SPACE) {
      return -1;
    }
    // insert into free space
    int slotId;
    if (result == hasEnoughSpaceResult.HAS_ENOUGH_SPACE) {
      setFreeSpacePointer(getFreeSpacePointer() - tuple.getSize());
      tuple.serialize(data_, getFreeSpacePointer());
      setTupleMeta(slotId = getSlotCount(), getFreeSpacePointer(), tuple.getSize(), SLOT_ALLOCATED);
      setSlotCount(getSlotCount() + 1);
    } else {
      // insert into deleted slots
      slotId = findDeletedSlot(tuple.getSize());
      tuple.serialize(data_, getTupleOffset(slotId));
      setTupleLength(slotId, tuple.getSize());
      markAllocated(slotId);
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
    if (slotId < 0 || slotId >= getSlotCount()) {
      return false;
    }
    if (!isAllocated(slotId)) {
      return false;
    }
    // set slot to 0
    markDelete(slotId);
    setTupleCount(getTupleCount() - 1);
    return true;
  }

  @Override
  // get tuple by slot id
  public Tuple getTuple(int slotId) {
    if (slotId < 0 || slotId >= getSlotCount()) {
      return null;
    }
    if (!isAllocated(slotId)) {
      return null;
    }
    return Tuple.deserialize(data_, getTupleOffset(slotId), getTupleLength(slotId));
  }

  // update tuple by slot id
  public boolean updateTuple(int slotId, Tuple tuple) {
    if (slotId < 0 || slotId >= getSlotCount()) {
      return false;
    }
    if (!isAllocated(slotId)) {
      return false;
    }
    tuple.serialize(data_, getTupleOffset(slotId));
    return true;
  }

  // we need tuple size
  @Override
  public void init(Object[] data) {
    setNextPageId(Global.PAGE_ID_INVALID);
    setPrevPageId(Global.PAGE_ID_INVALID);
    setFreeSpacePointer(Global.PAGE_SIZE);
    setTupleCount(0);
    setSlotCount(0);
  }

  @Override
  public int getSlotSize() {
    throw new UnsupportedOperationException();
  }

  // iterator
  public class TablePageIterator implements Iterator<Pair<Tuple, Integer>> {
    private int slotId = 0;

    @Override
    public boolean hasNext() {
      while (slotId < getSlotCount()) {
        if (isAllocated(slotId)) {
          return true;
        }
        slotId++;
      }
      return false;
    }

    @Override
    public Pair<Tuple, Integer> next() {
      int oldslot = slotId;
      slotId++;
      return new Pair<>(getTuple(oldslot), oldslot);
    }
  }

  // print all meta-info
  public void printmeta() {
    System.out.println("pageId: " + getPageId());
    System.out.println("nextPageId: " + getNextPageId());
    System.out.println("prevPageId: " + getPrevPageId());
    System.out.println("freeSpacePointer: " + getFreeSpacePointer());
    System.out.println("slotCount: " + getSlotCount());
    System.out.println("tupleCount: " + getTupleCount());
    for (int i = 0; i < getSlotCount(); i++) {
      System.out.println("slotId: " + i);
      System.out.println("tupleOffset: " + getTupleOffset(i));
      System.out.println("tupleLength: " + getTupleLength(i));
      System.out.println("tupleFlags: " + getTupleFlags(i));
    }
  }

  // iter
  @Override
  public Iterator<Pair<Tuple, Integer>> iterator() {
    return new TablePageIterator();
  }
}
