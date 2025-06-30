package replay.controller.util;

import lombok.Getter;
import replay.controller.executecontrol.*;

@Getter
public enum ReduceMutationType {
  DIVIDE_REDUCE(ExecuteFilterDivideReduce.class),
  DATA_GROUP(ExecuteFilterMutationDataGroup.class),
  DATA_GROUP_DIVIDE(ExecuteFilterMutationDataGroupDivide.class),
  OPERATION(ExecuteFilterMutationOperation.class),
  THREAD(ExecuteFilterMutationThread.class),
  THREAD_DIVIDE(ExecuteFilterMutationThreadDivide.class),
  DATA_GROUP_OP(ExecuteFilterMutationDataGroupOp.class),
  TRANSACTION(ExecuteFilterMutationTransaction.class),
  TRANSACTION_DIVIDE(ExecuteFilterMutationTransactionDivide.class),
  TRANSACTION_SERIAL(ExecuteFilterRandomTransactionSerial.class),
  CROP_ROLLBACK(ExecutionFilterCropRollback.class),
  CROP_TAIL_TRANSACTION(ExecutionFilterCropTailTxn.class),
  OPERATION_DIVIDE(ExecuteFilterMutationOperationDivide.class);

  private final Class<? extends ExecuteFilterMutationAbs> executeFilterClass;

  ReduceMutationType(Class<? extends ExecuteFilterMutationAbs> executeFilterClass) {
    this.executeFilterClass = executeFilterClass;
  }
}
