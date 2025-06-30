package util.rand;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class RandUtil {
  private final Random random;

  public RandUtil() {
    super();
    random = new Random();
  }

  public RandUtil(int seed) {
    super();
    random = new Random(seed);
    // 进入下一个随机状态
    random.nextInt();
  }

  public <T> T getRandElement(List<T> list) {
    return list.get(random.nextInt(list.size()));
  }

  /**
   * 根据概率抽取，概率用百分比表示
   *
   * @param itemMap 元素 : 百分比
   * @param <T> 类型
   * @return 抽取到的结果
   */
  public <T> T randSelectByProbability(Map<T, Integer> itemMap) {

    // 确保百分比之和为 100
    if (itemMap.values().stream().mapToInt(Integer::intValue).sum() != 100) {
      throw new RuntimeException("概率之和应当为1");
    }

    // 存储分隔值
    // 假如 指定的概率分别为 20 25 55
    // 那么分隔值就有4个，0 20 45 100
    // 抽取值位于闭后开区间即代表抽中该值
    // 比如33代表抽中第二个，45代表抽中第三个
    List<Integer> values = new ArrayList<>();
    List<T> targets = new ArrayList<>();
    values.add(0);
    int sum = 0;
    for (T key : itemMap.keySet()) {
      sum += itemMap.get(key);
      values.add(sum);
      targets.add(key);
    }

    // 抽取一个0-99的数字
    int randValue = new Random().nextInt(100);
    for (int i = 0; i < values.size(); i++) {
      if (randValue >= values.get(i) && randValue < values.get(i + 1)) {
        return targets.get(i);
      }
    }

    throw new RuntimeException("不应该运行到这里呀");
  }

  /**
   * 根据为true的概率产生true或者false
   *
   * @param probOfTrue true的概率
   * @return true or false
   */
  public boolean randBoolByPercent(int probOfTrue) {
    return random.nextInt(100) < probOfTrue;
  }

  public int nextInt() {
    return random.nextInt();
  }

  public int nextInt(int bound) {
    return random.nextInt(bound);
  }

  public double nextDouble() {
    return random.nextDouble();
  }
}
