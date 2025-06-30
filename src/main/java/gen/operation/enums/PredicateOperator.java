package gen.operation.enums;

import java.util.HashMap;
import java.util.Map;

public enum PredicateOperator {
  EQUAL_TO,
  GREATER_THAN,
  LESS_THAN,
  GREATER_EQUAL,
  LESS_EQUAL,
  NOT_EQUAL,
  LIKE,
  IN,
  BETWEEN,
  ISNULL;

  private static final Map<PredicateOperator, String> valueMap = new HashMap<>();

  static {
    valueMap.put(EQUAL_TO, " = ");
    valueMap.put(GREATER_THAN, " > ");
    valueMap.put(LESS_THAN, " < ");
    valueMap.put(GREATER_EQUAL, " >= ");
    valueMap.put(LESS_EQUAL, " <= ");
    valueMap.put(NOT_EQUAL, " != ");
    valueMap.put(LIKE, " like ");
    valueMap.put(IN, " in ");
    valueMap.put(BETWEEN, " between ");
    valueMap.put(ISNULL, " is null ");
  }

  public String getOperator() {
    return valueMap.get(this);
  }
}
