package util.rand;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.dom4j.Element;

public class RandUtils {
  private static final RandUtil randUtil = new RandUtil();

  public static <T> T getRandElement(List<T> list) {
    return randUtil.getRandElement(list);
  }

  /**
   * 根据概率抽取，概率用百分比表示
   *
   * @param itemMap 元素 : 百分比
   * @param <T> 类型
   * @return 抽取到的结果
   */
  public static <T> T randSelectByProbability(Map<T, Integer> itemMap) {

    return randUtil.randSelectByProbability(itemMap);
  }

  public static Integer randSelectByProbability(Element seedXML) {
    return randUtil.randSelectByProbability(getMapFromXML(seedXML));
  }

  /**
   * 从XML获取一个int枚举列表和概率的映射
   *
   * @param seedXML 包含type和probability两个整数列表的XML
   * @return
   */
  public static Map<Integer, Integer> getMapFromXML(Element seedXML) {
    String type = seedXML.element("type").getText();
    String probability = seedXML.element("probability").getText();

    return getMapFromString(type, probability);
  }

  /** 从map获取映射 */
  public static Map<Integer, Integer> getMapFromString(String type, String probability) {
    Map<Integer, Integer> numMap = new HashMap<>();
    String[] typeArray = type.split(",");
    String[] probabilityArray = probability.split(",");
    if (typeArray.length != probabilityArray.length) {
      throw new RuntimeException("类型数量和概率不对应");
    }
    if (typeArray.length == 0) {
      throw new RuntimeException("类型数量不得为零");
    }
    for (int i = 0; i < typeArray.length; ++i) {
      numMap.put(Integer.parseInt(typeArray[i]), Integer.parseInt(probabilityArray[i]));
    }
    return numMap;
  }

  /**
   * 根据为true的概率产生true或者false
   *
   * @param probOfTrue true的概率
   * @return true or false
   */
  public static boolean randBoolByPercent(int probOfTrue) {
    return randUtil.randBoolByPercent(probOfTrue);
  }

  public static int nextInt() {
    return randUtil.nextInt();
  }

  public static int nextInt(int bound) {
    return randUtil.nextInt(bound);
  }

  public static double nextDouble() {
    return randUtil.nextDouble();
  }
}
