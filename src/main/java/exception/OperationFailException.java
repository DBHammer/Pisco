package exception;

public class OperationFailException extends Exception {
  public OperationFailException() {
    super();
  }

  public OperationFailException(String message) {
    super(message);
  }

  public OperationFailException(String message, Throwable cause) {
    super(message, cause);
  }

  public OperationFailException(Throwable cause) {
    super(cause);
  }
}
