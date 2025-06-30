package replay.controller.pausecontrol.dependency;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import lombok.Getter;
import trace.OperationTraceType;

@Getter
public class OperationDependency {
  private final List<Integer> start;
  private final List<Integer> end;
  private final List<List<Integer>> operations;

  public OperationDependency() {
    start = new ArrayList<>(Arrays.asList(0, 0, 0));
    end = new ArrayList<>(Arrays.asList(0, 0, 0));

    operations = new ArrayList<>();
  }

  public void reset() {
    start.clear();
    end.clear();
    operations.clear();
  }

  public void update(OperationTraceType traceType, Dependency dependency) {
    if (traceType == OperationTraceType.START) {
      start.set(dependency.ordinal(), 1);
    } else if (traceType == OperationTraceType.COMMIT || traceType == OperationTraceType.ROLLBACK) {
      end.set(dependency.ordinal(), 1);
    } else {
      List<Integer> operation = new ArrayList<>(Arrays.asList(0, 0, 0));
      operation.set(dependency.ordinal(), 1);
      operations.add(operation);
    }
  }

  /**
   * merge to itself
   *
   * @param operationDependency
   */
  public void merge(OperationDependency operationDependency) {
    for (int i = 0; i < 3; ++i) {
      start.set(i, start.get(i) + operationDependency.getStart().get(i));
      end.set(i, end.get(i) + operationDependency.getEnd().get(i));
    }
    for (int i = 0; i < operations.size(); ++i) {
      for (int j = 0; j < 3; ++j) {
        operations
            .get(i)
            .set(j, operations.get(i).get(j) + operationDependency.getOperations().get(i).get(j));
      }
    }

    for (int i = operations.size(); i < operationDependency.getOperations().size(); ++i) {
      operations.add(new ArrayList<>(operationDependency.getOperations().get(i)));
    }
  }

  public boolean checkConsistent(int pos) {
    if (pos == 0) {
      return start.get(Dependency.After.ordinal()) == 0;
    } else if (pos == Integer.MAX_VALUE) {
      return end.get(Dependency.After.ordinal()) == 0;
    } else {
      return pos >= operations.size() || operations.get(pos).get(Dependency.After.ordinal()) == 0;
    }
  }
}
