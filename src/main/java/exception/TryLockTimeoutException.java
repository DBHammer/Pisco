package exception;

public class TryLockTimeoutException extends Exception {
  public TryLockTimeoutException() {
    super();
  }

  public TryLockTimeoutException(String message) {
    super(message);
  }

  public TryLockTimeoutException(String message, Throwable cause) {
    super(message, cause);
  }

  public TryLockTimeoutException(Throwable cause) {
    super(cause);
  }

  protected TryLockTimeoutException(
      String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }
}
