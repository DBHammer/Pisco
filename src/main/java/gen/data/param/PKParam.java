package gen.data.param;

import java.io.Serializable;

/** 存储主键某一列对应的生成参数 */
public class PKParam implements Serializable {
  // 考虑到这几个参数应当不允许修改，故设置为public final
  // 初始化时赋值
  // 后续引用时直接通过成员运算符引用即可
  public final int step; // pkId步长
  public final PKFunc pkFunc; // pkFunc

  public PKParam(int step, PKFunc pkFunc) {
    super();
    this.step = step;
    this.pkFunc = pkFunc;
  }
}
