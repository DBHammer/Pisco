package gen.operation;

import adapter.SQLFilter;
import gen.operation.basic.lockmode.LockMode;
import gen.operation.basic.orderby.SortKey;
import gen.operation.basic.project.Project;
import gen.operation.basic.where.PredicateLock;
import gen.operation.basic.where.WhereClause;
import gen.operation.enums.OperationLockMode;
import gen.operation.enums.OperationType;
import gen.operation.param.ParamInfo;
import gen.schema.generic.AbstractTable;
import io.SQLizable;
import java.io.Serializable;
import java.util.List;
import lombok.Data;
import org.apache.commons.lang3.NotImplementedException;

@Data
public abstract class Operation implements SQLizable, Serializable {
  // 记录操作类型
  private OperationType operationType;

  private Project project;
  private AbstractTable table;
  private LockMode lockMode = null; // 字符串
  private SortKey sortKey;
  private WhereClause whereClause;
  private List<ParamInfo> paramFillInfoList;

  // SQLFilter
  protected SQLFilter sqlFilter;

  public Operation(SQLFilter sqlFilter) {
    this.sqlFilter = sqlFilter;
  }

  public Operation(
      OperationType operationType,
      SQLFilter sqlFilter,
      Project project,
      AbstractTable table,
      LockMode lockMode,
      SortKey sortKey,
      WhereClause whereClause,
      List<ParamInfo> paramFillInfoList) {
    super();
    this.setOperationType(operationType);
    this.sqlFilter = sqlFilter;
    this.project = project;
    this.table = table;
    this.lockMode = lockMode;
    this.sortKey = sortKey;
    this.whereClause = whereClause;
    this.paramFillInfoList = paramFillInfoList;
  }

  /**
   * 返回隔离级别，供trace记录模块使用，不同于select中的lockMode字符串 todo 统一OperationLockMode和lockMode字符串
   *
   * @return OperationLockMode
   */
  public OperationLockMode getOperationLockMode() {
    if (lockMode != null) {
      return this.getLockMode().getOperationLockMode();
    }
    throw new NotImplementedException();
  }

  public ParamInfo findParamInfoByPredicate(PredicateLock predicate) {
    throw new NotImplementedException();
  }

  protected abstract String toSQLProtected();

  @Override
  public String toSQL() {
    return sqlFilter.filter(this.toSQLProtected());
  }

  @Override
  public String toString() {
    return this.toSQL();
  }
}
