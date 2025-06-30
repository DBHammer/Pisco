package util.time;

public class TimestampBase {
  public static final long millsTimestampBase = System.currentTimeMillis();
  public static final long nanoTimeBase = System.nanoTime();

  public static long nanoTime2MillsTimestamp(long nanoTime) {
    return millsTimestampBase + (nanoTime - nanoTimeBase) / 1000000;
  }
}
