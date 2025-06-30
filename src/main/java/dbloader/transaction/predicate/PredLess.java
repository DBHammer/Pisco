package dbloader.transaction.predicate;

import gen.operation.param.ParamInfo;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.Data;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode(callSuper = true)
@Data
public class PredLess extends PredGeneric {
  private final Integer val;
  private final Integer upperExclusive;

  public PredLess(String key, ParamInfo paramInfo, Integer val, Integer upperExclusive) {
    super(key, paramInfo, Collections.singletonList(val));
    this.val = val;
    this.upperExclusive = upperExclusive;
  }

  @Override
  public Set<Integer> toSet() {
    return IntStream.range(0, val).boxed().collect(Collectors.toSet());
  }
}
