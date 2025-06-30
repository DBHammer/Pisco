package dbloader.util;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class QueryExecutor {
  public static final ExecutorService executor = Executors.newCachedThreadPool();
}
