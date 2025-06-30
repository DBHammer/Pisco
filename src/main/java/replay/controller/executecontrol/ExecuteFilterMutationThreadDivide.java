package replay.controller.executecontrol;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;
import trace.OperationTrace;

public class ExecuteFilterMutationThreadDivide extends ExecuteFilterMutationThread {

  @Override
  public void initializeWithAllTxn(List<List<List<OperationTrace>>> allTxns) {
    super.initializeWithAllTxn(allTxns);
    executeThreadQueue = new LinkedList<>();
    executeThreadQueue.add(
        allTxns.stream()
            .parallel()
            .filter(thread -> thread != null && !thread.isEmpty() && !thread.get(0).isEmpty())
            .map(thread -> thread.get(0).get(0).getThreadID())
            .collect(Collectors.toList()));

    mutation();
    revertMutation();
  }

  @Override
  public void revertMutation() {
    // 如果只剩一个线程，就不用再切了
    if (currentThread.size() == 1) {
      currentThread.clear();
      return;
    }
    // 计算每个子列表的基本大小
    int groupSize = (int) Math.sqrt(currentThread.size()) - 1; // /2;

    // 记录当前切分的起始索引
    int currentIndex = 0;
    // 计算当前子列表的大小
    int size = groupSize > 0 ? groupSize : 1;

    // 循环切分列表
    while (currentIndex + size <= currentThread.size()) {

      // 使用 sublist 方法获取当前子列表
      List<String> subList =
          new ArrayList<>(currentThread.subList(currentIndex, currentIndex + size));
      executeThreadQueue.add(subList);

      // 更新当前切分的起始索引
      currentIndex += size;
    }
    // 特判最后一组，因为数量可能不齐
    if (currentIndex < currentThread.size()) {
      executeThreadQueue.add(
          new ArrayList<>(currentThread.subList(currentIndex, currentThread.size())));
    }

    currentThread.clear();
  }
}
