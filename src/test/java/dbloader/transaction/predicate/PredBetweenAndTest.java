package dbloader.transaction.predicate;

import java.util.HashSet;
import java.util.Random;
import org.junit.Test;

public class PredBetweenAndTest {

  @Test
  public void toSet() {
    for (int i = 0; i < 10; ++i) {
      int start = new Random().nextInt(100);
      int gap = new Random().nextInt(100);
      PredBetweenAnd test1 = new PredBetweenAnd("0", null, start, start + gap, -1);
      HashSet<Integer> set = new HashSet<>();
      for (int j = start; j <= start + gap; ++j) {
        set.add(j);
      }
      assert test1.toSet().equals(set);
    }
  }
}
