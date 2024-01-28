package neptune.common.exception;

public class DuplicateKeyException extends RuntimeException {
  @Override
  public String getMessage() {
    return "Exception: insertion caused duplicated keys!";
  }
}
