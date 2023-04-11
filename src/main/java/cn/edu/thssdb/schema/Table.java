package cn.edu.thssdb.schema;

import cn.edu.thssdb.index.BPlusTree;
import cn.edu.thssdb.storage.Tuple;
import cn.edu.thssdb.utils.Pair;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.locks.ReentrantReadWriteLock;

// TODO: Rewrite this class
public class Table implements Iterable<Tuple> {
  ReentrantReadWriteLock lock;
  private final TableInfo info_;

  @Deprecated public BPlusTree<Entry, Row> index;
  @Deprecated private int primaryIndex;

  public Table(TableInfo info) {
    this.info_ = info;
    this.lock = new ReentrantReadWriteLock();
  }

  private void recover() {
    // TODO
  }

  public void insert() {
    // TODO
  }

  public void delete() {
    // TODO
  }

  public void update() {
    // TODO
  }

  private void serialize() {
    // TODO
  }

  private ArrayList<Row> deserialize() {
    // TODO
    return null;
  }

  private class TableIterator implements Iterator<Tuple> {
    private Iterator<Pair<Entry, Tuple>> iterator;

    TableIterator(Table table) {
      // TODO
    }

    @Override
    public boolean hasNext() {
      return iterator.hasNext();
    }

    @Override
    public Tuple next() {
      return iterator.next().right;
    }
  }

  @Override
  public Iterator<Tuple> iterator() {
    return new TableIterator(this);
  }
}
