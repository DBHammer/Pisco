package replay.controller.pausecontrol.serial;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.stream.Collectors;
import lombok.Getter;
import replay.controller.ReplayController;
import trace.OperationTrace;
import trace.TraceUtil;

/**
 * 存储和使用存储序列的类。 操作序列指对所有操作按一定规则推断后得到的序列，这个序列由若干个操作集合组成，集合内的操作可以以任意顺序并行执行，各集合间按序列顺序串行执行。 本类提供两种功能： 1.
 * 判断和维护当前执行的序列，从而使负载按操作序列的顺序执行 2. 处理各种依赖操作顺序的数据
 */
public class OperationSequence {

  // 线程数
  @Getter private final int threadNum;
  // 操作序列
  @Getter private List<List<String>> operationSeq;

  // 指向当前需要执行的操作位置的指针
  private AtomicInteger cursor = new AtomicInteger(0);

  // 记录已经发送的操作
  private CopyOnWriteArraySet<String> executedOperation;

  private AtomicIntegerArray executedOpNumber;

  // 维护每个序列的锁结构
  private List<ConcurrentLinkedQueue<String>> lockList = new CopyOnWriteArrayList<>();
  // 辅助结构，用来存每个操作在序列上的位置
  Map<String, Integer> id2Cursor;

  // 这个监听周期有没有推进过
  private AtomicBoolean pushed = new AtomicBoolean(false);

  /**
   * 初始化
   *
   * @param thread 负载的线程数
   * @param operations 操作序列
   */
  public OperationSequence(int thread, List<List<String>> operations) {
    threadNum = thread;
    operationSeq = operations;

    reset();
  }

  /** 初始化数据结构 */
  public void reset() {
    cursor = new AtomicInteger(0);
    id2Cursor = new HashMap<>();
    lockList = new CopyOnWriteArrayList<>();
    executedOpNumber = new AtomicIntegerArray(operationSeq.size());
    for (int i = 0; i < operationSeq.size(); i++) {
      for (String opId : operationSeq.get(i)) {
        id2Cursor.put(opId, i); // < operation_id, trx_id >
      }
      lockList.add(new ConcurrentLinkedQueue<>());
      executedOpNumber.set(i, 0);
    }
    executedOperation = new CopyOnWriteArraySet<>();
  }

  /**
   * 拷贝构造
   *
   * @param seq 另一个操作序列对象
   */
  public OperationSequence(OperationSequence seq) {
    threadNum = seq.getThreadNum();
    operationSeq = new ArrayList<>(seq.getOperationSeq());
    pushed = seq.pushed;

    reset();
  }

  /**
   * 获得最后一个错误trace执行后的op id
   *
   * @param errorTraces 错误trace的集合
   * @return op id
   */
  public Set<String> getTailOpId(Set<OperationTrace> errorTraces) {
    // 线程当前执行到的事务id
    Set<String> tailOp = new HashSet<>();

    Set<Integer> tailTxnStart = new HashSet<>();

    int exeCnt = 0;
    int scanCursor = 0;
    // 扫描已经执行的部分
    for (; scanCursor < operationSeq.size(); scanCursor++) {
      List<String> currentSet = operationSeq.get(scanCursor);
      // 如果错误trace已经都经过了，结束扫描
      if (exeCnt >= 1) {
        for (String opId : currentSet) {
          int threadID = TraceUtil.getThreadIdFromOpId(opId);
          int opIdx = Integer.parseInt(opId.split(",")[2]);
          // 如果是start
          if (opIdx == 0) {
            tailTxnStart.add(threadID);
          }

          if (tailTxnStart.contains(threadID)) {
            tailOp.add(opId);
          }
        }
      }
      // 计数已经经过的错误trace的数量
      for (OperationTrace errorTrace : errorTraces) {
        if (currentSet.contains(errorTrace.getOperationID())) exeCnt++;
      }
    }
    return tailOp;
  }

  /** 过滤需要执行的操作，使操作序列仅包含需要执行的部分 */
  public void filterExecuteOp() {
    List<List<String>> finalOp = new ArrayList<>();
    // 遍历
    for (List<String> strings : operationSeq) {
      List<String> ops = new ArrayList<>();
      for (String operation : strings) {

        // 判断是否需要执行
        if (ReplayController.isExecute(operation)) {
          ops.add(operation);
        }
      }
      // 如果当前集合中的操作都不需要执行，直接跳过
      if (!ops.isEmpty()) {
        finalOp.add(ops);
      }
    }

    // 用需要执行的部分替换全部
    operationSeq = finalOp;
  }

  /** 输出操作序列 */
  public void print() {
    System.out.println("Operation sequence: " + operationSeq.toString());
  }

  /** 展平操作序列，使每个集合内仅存在一个操作，实现真正意义上的串行 */
  public void flatten() {
    List<List<String>> finalOp = new ArrayList<>();
    for (List<String> strings : operationSeq) {
      for (String operation : strings) {
        // 把每个操作当作一个单独的集合加进序列里，同个集合的操作不限制顺序
        finalOp.add(new ArrayList<>(Collections.singleton(operation)));
      }
    }

    operationSeq = finalOp;
    // 需要重新初始化数据结构，因为与序列相关的信息发生了改变
    reset();
  }

  /**
   * 维护执行顺序的核心函数，输入需要执行的操作 如果当前操作可以立即执行（位于不晚于操作序列需要执行的位置），则标记为已发送，且直接返回 如果不可以立即执行（位于当前执行集合之后），则等待
   *
   * @param operationId 需要执行的操作
   */
  public void execute(String operationId) {
    // 当前操作在序列上的位置
    Integer executeCursor = id2Cursor.get(operationId);

    if (executeCursor == null) {
      return;
    }

    // 考虑到有些操作可能被跳掉，在这里额外更新一下已完成操作的信息，把同个线程之前的操作全部标记为已完成
    // 当前操作所在的线程
    String threadId = operationId.split(",")[0];
    // 从执行位置到当前操作位置遍历序列的内容，因为这部分操作是被跳过的

    // executedOperation 添加上一个执行位置到当前执行位置，被跳过的操作
    // executedOpNumber 每个组已完成或跳过的操作的数量
    int finishCursor = executeCursor - 1;
    while (finishCursor >= 0) {
      Set<String> skipOps =
          operationSeq.get(finishCursor).stream()
              .filter(id -> id.startsWith(threadId) && !executedOperation.contains(id))
              .collect(Collectors.toSet());

      if (!skipOps.isEmpty()) {
        for (String skipOp : skipOps) {
          int skipCursor = id2Cursor.get(skipOp);
          executedOpNumber.updateAndGet(skipCursor, count -> count + 1);
        }
        executedOperation.addAll(skipOps);
      }
      finishCursor--;
    }

    // 更新执行位置
    updateCursor();

    // 如果更新后当前事务的位置仍然晚于执行位置，则等待
    if (executeCursor > cursor.get()) {

      ConcurrentLinkedQueue<String> waitingQueue = lockList.get(executeCursor);
      waitingQueue.add(operationId);

      try {
        // 使用 Condition 等待，而不是直接阻塞线程
        while (executeCursor > cursor.get()) {
          synchronized (waitingQueue) {
            waitingQueue.wait();
          }
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      }
    }
    // 到这里说明已经可以执行了，把操作放进可执行操作里
    pushed.set(true);
  }

  public void executeFinish(String operationId) {
    Integer skipCursor = id2Cursor.get(operationId);
    if (skipCursor == null) {
      return;
    }

    if (!executedOperation.contains(operationId)) {
      // 使用 AtomicIntegerArray 来更新 executedOpNumber
      executedOpNumber.updateAndGet(skipCursor, count -> count + 1);
    }
    executedOperation.add(operationId);
    updateCursor();
  }
  //  public void executeFinish(String operationId) {
  //
  //    Integer skipCursor = id2Cursor.get(operationId);
  //
  //    if (skipCursor == null){
  //      return;
  //    }
  //
  //    if (!executedOperation.contains(operationId)) {
  //      // 增加已执行操作
  //      synchronized (executedOpNumber) {
  //        executedOpNumber.set(skipCursor, executedOpNumber.get(skipCursor) + 1);
  //      }
  //    }
  //    executedOperation.add(operationId);
  //    updateCursor();
  //  }

  /** 更新执行位置，把位置指针移动到已全部完成的操作序列之后 */
  //  private synchronized void updateCursor() {
  //    int oldCursor = cursor;
  //    // 移动指针
  //    while (cursor < operationSeq.size()
  //        && executedOpNumber.get(cursor) >= operationSeq.get(cursor).size()) {
  //      // executedOperation.containsAll(operationSeq.get(cursor)
  //      cursor++;
  //    }
  //    // 把当前执行位置之前的锁全部唤醒，按执行序列的顺序唤醒，存在微弱的乱序可能，需要进一步证明 TODO
  //    synchronized (lockList) {
  //      for (int i = oldCursor; i <= cursor && i < lockList.size(); i++) {
  //        for (Object lock : lockList.get(i)) {
  //          synchronized (lock) {
  //            lock.notifyAll();
  //          }
  //        }
  //        // 清空锁表中被唤醒的部分
  //        //                lockList.get(i).clear();
  //      }
  //    }
  //  }

  private void updateCursor() {
    while (true) {
      int currentCursor = cursor.get();
      if (currentCursor >= operationSeq.size()) {
        break;
      }

      int opCount = operationSeq.get(currentCursor).size();
      int executedCount = executedOpNumber.get(currentCursor);

      if (executedCount >= opCount) {
        if (cursor.compareAndSet(currentCursor, currentCursor + 1)) {
          // 唤醒当前 cursor + 1 位置的等待队列
          int newCursor = currentCursor + 1;
          if (newCursor < lockList.size()) {
            ConcurrentLinkedQueue<String> waitingQueue = lockList.get(newCursor);
            synchronized (waitingQueue) {
              waitingQueue.notifyAll();
            }
          }
        }
      } else {
        break;
      }
    }
  }

  /** 最终保险，直接推进执行位置，在程序卡死时由主程序调用 找到下一个可以被执行的操作 => cursor，将上一个执行位置的组到当前的组的所有锁全部唤醒 */
  public void moveCursor() {
    if (pushed.get()) {
      pushed.set(false);
      return;
    }

    AtomicInteger oldCursor = new AtomicInteger(cursor.get());
    boolean needExecute = false;
    while (!needExecute) {
      int newCursor = cursor.get() + 1;
      if (!cursor.compareAndSet(cursor.get(), newCursor)) {
        continue;
      }

      if (newCursor >= lockList.size()) {
        for (int i = oldCursor.get(); i < lockList.size(); i++) {
          ConcurrentLinkedQueue<String> waitingQueue = lockList.get(i);
          synchronized (waitingQueue) {
            waitingQueue.notifyAll();
          }
        }
        break;
      }

      for (String opId : operationSeq.get(newCursor)) {
        if (ReplayController.isExecute(opId)) {
          needExecute = true;
          break;
        }
      }
    }

    for (int i = oldCursor.get(); i <= cursor.get() && i < lockList.size(); i++) {
      ConcurrentLinkedQueue<String> waitingQueue = lockList.get(i);
      synchronized (waitingQueue) {
        waitingQueue.notifyAll();
      }
    }

    updateCursor();
  }
  //  public synchronized void moveCursor() {
  //
  //    if (pushed) {
  //      pushed = false;
  //      return;
  //    }
  //    pushed = false;
  //    System.out.printf("force move cursor: %d/%d\n", cursor, lockList.size());
  //
  //    AtomicInteger oldCursor = cursor;
  //    boolean needExecute = false;
  //    while (!needExecute) {
  //      cursor.set(cursor.get() + 1);
  //      if (cursor.get() >= lockList.size()) {
  //        for (int i = oldCursor.get(); i < lockList.size(); i++) {
  //          for (Object lock : lockList.get(i)) {
  //            synchronized (lock) {
  //              lock.notifyAll();
  //            }
  //          }
  //          //                lockList.get(i).clear();
  //        }
  //        break;
  //      }
  //      for (String opId : operationSeq.get(cursor.get())) {
  //        if (ReplayController.isExecute(opId)) {
  //          needExecute = true;
  //          break;
  //        }
  //      }
  //    }
  //
  //    for (int i = oldCursor.get(); i <= cursor.get() && i < lockList.size(); i++) {
  //      for (Object lock : lockList.get(i)) {
  //        synchronized (lock) {
  //          lock.notifyAll();
  //        }
  //      }
  //      //            lockList.get(i).clear();
  //    }
  //
  //    updateCursor();
  //  }
}
