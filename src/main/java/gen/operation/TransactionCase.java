package gen.operation;

import adapter.SQLFilter;
import config.schema.TransactionConfig;
import context.OrcaContext;
import gen.operation.basic.*;
import gen.operation.enums.OperationType;
import gen.operation.enums.StartType;
import gen.schema.Schema;
import io.IOUtils;
import io.SQLizable;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import lombok.Data;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import util.xml.Seed;

@Data
public class TransactionCase implements SQLizable, Serializable {
  private final int transactionID;
  private final List<Operation> operationList;

  public TransactionCase(
      int transactionID, TransactionConfig operationConfig, Schema schema, SQLFilter sqlFilter) {
    this.transactionID = transactionID;

    Seed operationNumberSeed = operationConfig.getOperationNumber();

    TransactionConfig.StartConfig startConfig = operationConfig.getStartConfig();
    Seed readOnlySeed = startConfig.getReadOnlySeed();
    int operationNumber = operationNumberSeed.getRandomValue();
    // 按种子随机生成CRUD中的一个操作
    this.operationList = new ArrayList<>();

    Seed operationTypeSeed = operationConfig.getOperationTypeSeed();
    int operationType;
    operationType = operationTypeSeed.getRandomValue();
    if (operationType == OperationType.DDL.ordinal()) {
      operationList.add(new OperationDDL(sqlFilter, schema, operationConfig.getDDL()));
      return;
    }
    if (operationType == OperationType.Fault.ordinal()) {
      operationList.add(new OperationFault(sqlFilter, schema, OrcaContext.configColl.getFault()));
      return;
    }
    if (operationType == OperationType.DistributeSchedule.ordinal()
        && OrcaContext.configColl.getSchema().getTable().isUsePartition()) {
      operationList.add(
          new OperationDistributeSchedule(sqlFilter, schema, operationConfig.getDDL()));
      return;
    }

    StartType startType;
    Seed operationStartTypeSeed = startConfig.getSnapshotSeed();
    // 如果是read only，只生成select语句
    switch (readOnlySeed.getRandomValue()) {
      case 0:
        // 考虑要不要生成快照
        if (operationStartTypeSeed.getRandomValue() == 0)
          startType = StartType.StartTransactionReadOnlyWithConsistentSnapshot;
        else startType = StartType.StartTransactionReadOnly;
        break;
      case 1:
        // 考虑要不要生成快照
        if (operationStartTypeSeed.getRandomValue() == 0)
          startType = StartType.StartTransactionWithConsistentSnapshot;
        else startType = StartType.START;
        break;
      default:
        startType = StartType.START;
    }
    operationList.add(new OperationStart(startType));

    for (int i = 1; i <= operationNumber; i++) {
      if (startType == StartType.StartTransactionReadOnlyWithConsistentSnapshot
          || startType == StartType.StartTransactionReadOnly) {
        this.operationList.add(
            new OperationSelect(sqlFilter, schema, operationConfig.getSelect(), startType));
        continue;
      }
      // 每次生成的时候都刷新
      operationType = operationTypeSeed.getRandomValue();
      switch (operationType) {
        case 0:
          this.operationList.add(
              new OperationInsert(sqlFilter, schema, operationConfig.getInsert()));
          break;
        case 1:
          this.operationList.add(
              new OperationDelete(sqlFilter, schema, operationConfig.getDelete()));
          break;
        case 2:
          this.operationList.add(
              new OperationUpdate(sqlFilter, schema, operationConfig.getUpdate()));
          break;
        case 3:
          this.operationList.add(
              new OperationSelect(sqlFilter, schema, operationConfig.getSelect(), startType));
          break;
        default:
          i--;
          break;
          //                case 4:
          //                    this.operationList.add(new ReadYourWrite(i, schema, operationXML));
          //                case 5:
          //                    this.operationList.add(new ReadModifyWrite(i, schema,
          // operationXML));
          //                    break;
          //                case 6:
          //                    this.operationList.add(new ReadTwice(i, schema, operationXML));
          //                    break;
          //                default:
          //                    this.operationList.add(new UpdateReference(i, schema,
          // operationXML));
          //                    break;
      }
    }
    operationList.add(new OperationCommit());
  }

  public TransactionCase(int transactionID, List<Operation> operationList) {
    this.transactionID = transactionID;
    this.operationList = operationList;
  }

  @Override
  public String toSQL() {
    ArrayList<String> operationSQLs = new ArrayList<>();
    for (Operation operation : operationList) {
      if (operation.toSQL().isEmpty()) {
        Logger logger = LogManager.getLogger(getClass());
        logger.warn(
            String.format("%s.toSQL() returns empty", operation.getClass().getCanonicalName()));
      } else {
        operationSQLs.add(operation.toSQL());
      }
    }
    return String.join("\n", operationSQLs);
  }

  public String toString() {
    return toSQL();
  }

  public void writeSQL(String dest) throws IOException {
    IOUtils.writeString(this.toSQL(), dest, false);
  }
}
