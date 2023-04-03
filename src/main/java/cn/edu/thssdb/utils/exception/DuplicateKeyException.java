package cn.edu.thssdb.utils.exception;

public class DuplicateKeyException extends RuntimeException {
  @Override
  public String getMessage() {
    return "Exception: insertion caused duplicated keys!";
  }
}
