package gen.data.param;

/** attrFunc接口 */
public interface AttrFunc {
  /**
   * 根据输入的pkId确定性的计算出一个结果(要求可以逆向计算出pkId)
   *
   * @param pkId pkId
   * @return 称为 result
   */
  int calc(int pkId);
}
