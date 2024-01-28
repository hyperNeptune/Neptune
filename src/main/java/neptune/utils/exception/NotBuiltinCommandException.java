package neptune.utils.exception;

public class NotBuiltinCommandException extends RuntimeException {
  @Override
  public String getMessage() {
    return "Error: Not a built-in command.";
  }

  @Override
  public String toString() {
    return getMessage();
  }
}
