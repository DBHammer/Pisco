package dbloader.transaction.predicate;

import java.util.HashSet;
import org.junit.Test;

public class PredLessTest {

  @Test
  public void toSet() {
    for (int i = 0; i < 10; ++i) {
      PredLess test1 = new PredLess("0", null, i, -1);
      HashSet<Integer> set = new HashSet<>();
      for (int j = 0; j < i; ++j) {
        set.add(j);
      }
      assert test1.toSet().equals(set);
    }
  }
}
