package exception;

public class DeleteFailException extends Exception {
  public DeleteFailException() {
    super();
  }

  public DeleteFailException(String message) {
    super(message);
  }

  public DeleteFailException(String message, Throwable cause) {
    super(message, cause);
  }

  public DeleteFailException(Throwable cause) {
    super(cause);
  }

  protected DeleteFailException(
      String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }
}
