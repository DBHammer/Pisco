package trace;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** 对 TransactionTraceReader 进行一层缓存，可以用于在存在 SavePoint 时过滤掉被回滚的操作 */
public class BufferedTransactionTraceReader {

  private final TransactionTraceReader reader;
  private final List<OperationTrace> bufferedTraces = new ArrayList<>();

  public BufferedTransactionTraceReader(String src) {
    reader = new TransactionTraceReader(src);
  }

  public void begin() throws IOException {
    reader.begin();
  }

  public boolean hasNext() throws IOException {
    if (!bufferedTraces.isEmpty()) {
      return true;
    }

    while (reader.hasNext()) {

      OperationTrace trace = reader.readOperationTrace();

      if (trace.getOperationTraceType() == OperationTraceType.ROLLBACK) {
        bufferedTraces.add(trace);
        return true;
      } else if (trace.getOperationTraceType() == OperationTraceType.COMMIT) {
        // 若 rollback to savepoint
        // 则忽略被回滚的操作
        String savePoint = trace.getSavepoint();
        if (savePoint != null) {
          int operationSequence = Integer.parseInt(savePoint.split(",")[2]);
          bufferedTraces.removeIf(
              tr -> Integer.parseInt(tr.getOperationID().split(",")[2]) > operationSequence);
        }
        bufferedTraces.add(trace);
        return true;
      } else if (trace.getOperationTraceType() == OperationTraceType.DDL
          || trace.getOperationTraceType() == OperationTraceType.DistributeSchedule) {
        bufferedTraces.add(trace);
        return true;
      } else {
        bufferedTraces.add(trace);
      }
    }

    return reader.hasNext();
  }

  public OperationTrace readOperationTrace() {
    OperationTrace trace = bufferedTraces.get(0);
    bufferedTraces.remove(0);
    return trace;
  }
}
