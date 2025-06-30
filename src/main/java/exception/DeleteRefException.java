package exception;

public class DeleteRefException extends Exception {
  public DeleteRefException() {
    super();
  }

  public DeleteRefException(String message) {
    super(message);
  }

  public DeleteRefException(String message, Throwable cause) {
    super(message, cause);
  }

  public DeleteRefException(Throwable cause) {
    super(cause);
  }

  protected DeleteRefException(
      String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }
}
