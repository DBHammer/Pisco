package dbloader.transaction.predicate;

import java.util.HashSet;
import java.util.Random;
import org.junit.Test;

public class PredGreaterTest {

  @Test
  public void toSet() {
    for (int i = -10; i < 10; ++i) {
      int gap = new Random().nextInt(100);
      PredGreater test1 = new PredGreater("0", null, i, i + gap);
      HashSet<Integer> set = new HashSet<>();
      for (int j = i + 1; j < i + gap; ++j) {
        set.add(j);
      }
      assert test1.toSet().equals(set);
    }
  }
}
