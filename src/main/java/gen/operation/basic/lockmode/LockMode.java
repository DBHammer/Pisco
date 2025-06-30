package gen.operation.basic.lockmode;

import gen.operation.enums.OperationLockMode;
import io.SQLizable;
import java.io.Serializable;
import lombok.Getter;
import util.jdbc.DataSourceUtils;
import util.xml.Seed;

@Getter
public class LockMode implements SQLizable, Serializable {
  private final OperationLockMode operationLockMode; // 枚举值，供trace记录模块使用

  public LockMode(Seed lockModeSeed) {
    // 随机选择是哪种锁或者不加锁
    int lockMode = lockModeSeed.getRandomValue();
    if (lockMode > 2) {
      operationLockMode = OperationLockMode.SELECT;
    } else {
      operationLockMode = OperationLockMode.values()[lockMode];
    }
  }

  public LockMode(OperationLockMode operationLockMode) {
    this.operationLockMode = operationLockMode;
  }

  public LockMode() {
    operationLockMode = OperationLockMode.SELECT;
  }

  public String toString() {
    return toSQL();
  }

  @Override
  public String toSQL() {
    return DataSourceUtils.getAdapter().lockMode(operationLockMode);
  }
}
