package dbloader.transaction.predicate;

import java.util.HashSet;
import org.junit.Test;

public class PredEqualTest {

  @Test
  public void toSet() {
    for (int i = -10; i < 10; ++i) {
      PredEqual test1 = new PredEqual("0", null, i, -1);
      HashSet<Integer> set = new HashSet<>();
      set.add(i);
      assert test1.toSet().equals(set);
    }
  }
}
