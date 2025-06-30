package gen.data.param.func;

import gen.data.param.AttrFunc;
import org.junit.Test;

public class SimpleAttrFuncTest {

  @Test
  public void test() {
    AttrFunc attrFunc = new SimpleAttrFunc(1);
    for (int pkId = 0; pkId < 100; pkId++) {
      System.out.printf("%d - %d\n", pkId, attrFunc.calc(pkId));
    }
  }
}
