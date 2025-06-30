package dbloader.transaction.predicate;

import gen.operation.param.ParamInfo;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.Data;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode(callSuper = true)
@Data
public class PredBetweenAnd extends PredGeneric {
  private final Integer startInclusive;
  private final Integer endInclusive;
  private final Integer upperExclusive;

  public PredBetweenAnd(
      String key,
      ParamInfo paramInfo,
      Integer startInclusive,
      Integer endInclusive,
      Integer upperExclusive) {
    super(key, paramInfo, Arrays.asList(startInclusive, endInclusive));
    this.startInclusive = startInclusive;
    this.endInclusive = endInclusive;
    this.upperExclusive = upperExclusive;
  }

  @Override
  public Set<Integer> toSet() {
    return IntStream.rangeClosed(startInclusive, endInclusive).boxed().collect(Collectors.toSet());
  }
}
