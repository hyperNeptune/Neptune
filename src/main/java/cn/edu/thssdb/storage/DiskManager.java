package cn.edu.thssdb.storage;

import cn.edu.thssdb.utils.Global;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.*;

// this class manages the disk
public class DiskManager {
  Path path_;
  SeekableByteChannel file_;
  private int numPages_;

  // constructor
  public DiskManager(Path path) throws IOException {
    path_ = path;
    // open the file named filename, create it if not exists.
    // if you cannot create it, throw an exception.
    // it needs to be a binary mode file.
    file_ =
        Files.newByteChannel(
            path, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
    numPages_ = (int) (file_.size() / Global.PAGE_SIZE);
  }

  // read one page from file_ with page_id to Page p
  // if page_id is invalid, throw an exception
  // if you cannot read, throw an exception
  // return the page
  // params: int page_id, Page p
  public Page readPage(int page_id, Page p) throws IOException {
    if (page_id < 0) {
      throw new IOException("Invalid page id " + page_id);
    }
    long offset = (long) page_id * Global.PAGE_SIZE;
    if (offset >= file_.size()) {
      throw new IOException("file size is " + file_.size() + ", but offset is " + offset);
    }
    file_.position(offset);
    file_.read(p.getData());
    return p;
  }

  // write one page from Page p to file_ with page_id
  // if page_id is invalid, throw an exception
  // if you cannot write, throw an exception
  // return the page
  // params: int page_id, Page p
  public Page writePage(int page_id, Page p) throws IOException {
    if (page_id < 0) {
      throw new IOException("Invalid page id " + page_id);
    }
    long offset = (long) page_id * Global.PAGE_SIZE;
    file_.position(offset);
    ByteBuffer buf = p.getData();
    buf.rewind();
    file_.write(buf);
    return p;
  }

  // allocatePage
  public int allocatePage() {
    return numPages_++;
  }
}
