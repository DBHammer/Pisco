package gen.operation;

import adapter.SQLFilter;
import config.schema.TransactionConfig;
import context.OrcaContext;
import gen.schema.Schema;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import util.rand.RandUtil;

@Getter
public class TransactionCaseRepo extends ArrayList<TransactionCase> implements Serializable {

  private List<List<Integer>> caseSequence;

  public TransactionCaseRepo(
      int sizeOfTransactionRepo,
      TransactionConfig operationConfig,
      Schema schema,
      SQLFilter sqlFilter) {
    super();
    for (int transactionId = 0; transactionId < sizeOfTransactionRepo; transactionId++) {
      TransactionCase transactionCase =
          new TransactionCase(transactionId, operationConfig, schema, sqlFilter);
      this.add(transactionCase);
    }

    // 如果是指定template数量，我们预先生成template的序列，使执行时模板提前可知并固定，便于后续的变异
    if (OrcaContext.configColl.getLoader().getNumberOfTransactionCase() > 0) {
      caseSequence = new ArrayList<>();
      RandUtil randUtil = new RandUtil();

      for (int i = 0; i < OrcaContext.configColl.getLoader().getNumberOfLoader(); i++) {
        List<Integer> tmp = new ArrayList<>();
        for (int j = 0; j < OrcaContext.configColl.getLoader().getExecCountPerLoader(); j++) {
          tmp.add(randUtil.nextInt(sizeOfTransactionRepo));
        }
        caseSequence.add(tmp);
      }
    }
  }

  public TransactionCaseRepo(List<TransactionCase> transactionCases) {
    super();
    this.addAll(transactionCases);
  }

  public void writeSQL(String outputDir) throws IOException {
    for (TransactionCase transactionCase : this) {
      String dest =
          String.format(
              "%s/transaction_template/transaction_template_%d.sql",
              outputDir, transactionCase.getTransactionID());
      // 存储 transactionCase
      transactionCase.writeSQL(dest);
    }
  }
}
