package replay.controller.executecontrol;

import java.util.ArrayList;
import java.util.List;

public class ExecuteFilterMutationTransactionDivide extends ExecuteFilterMutationTransaction {
  // 需要执行的事务列表

  @Override
  public boolean isMutationEnd() {
    return super.isMutationEnd();
  }

  @Override
  public boolean isExecute(int threadNo, int transactionNo, String operationId) {
    return super.isExecute(threadNo, transactionNo, operationId);
  }

  @Override
  public void revertMutation() {
    // 如果只剩一个线程，就不用再切了
    if (currentTxn.size() == 1) {
      currentTxn.clear();
      return;
    }
    // 计算每个子列表的基本大小
    int groupSize = (int) currentTxn.size() / 2;

    // 记录当前切分的起始索引
    int currentIndex = 0;
    // 计算当前子列表的大小
    int size = groupSize > 0 ? groupSize : 1;

    // 循环切分列表
    while (currentIndex + size <= currentTxn.size()) {

      // 使用 sublist 方法获取当前子列表
      List<String> subList = new ArrayList<>(currentTxn.subList(currentIndex, currentIndex + size));
      executeTxnQueue.add(subList);

      // 更新当前切分的起始索引
      currentIndex += size;
    }
    // 特判最后一组，因为数量可能不齐
    if (currentIndex < currentTxn.size()) {
      executeTxnQueue.add(new ArrayList<>(currentTxn.subList(currentIndex, currentTxn.size())));
    }

    currentTxn.clear();
  }
}
