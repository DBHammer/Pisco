package gen.data.param;

/** 作为 PKF 的接口，方便修改 PKF 实现 */
public interface PKFunc {
  /**
   * 根据输入的pkId确定性的计算出一个result
   *
   * @param pkId pkId
   * @return 称为 result
   */
  int calc(int pkId);
}
