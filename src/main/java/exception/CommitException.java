package exception;

public class CommitException extends Exception {
  public CommitException() {
    super();
  }

  public CommitException(String message) {
    super(message);
  }

  public CommitException(String message, Throwable cause) {
    super(message, cause);
  }

  protected CommitException(
      String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }

  public CommitException(Throwable cause) {
    super(cause);
  }
}
