package dbloader.transaction.predicate;

import gen.operation.param.ParamInfo;
import java.util.List;
import java.util.Set;
import lombok.Data;

@Data
public abstract class PredGeneric {
  /** key */
  private final String key;

  private final ParamInfo paramInfo;
  private final List<Integer> logicalValues;

  protected PredGeneric() {
    key = null;
    paramInfo = null;
    logicalValues = null;
  }

  protected PredGeneric(String key, ParamInfo paramInfo, List<Integer> logicalValues) {
    this.key = key;
    this.paramInfo = paramInfo;
    this.logicalValues = logicalValues;
  }

  /**
   * 返回Predicate代表的集合
   *
   * @return set
   */
  public abstract Set<Integer> toSet();

  /*
   该Predicate是否有效
   @return true for valid, false for invalid
  */
}
