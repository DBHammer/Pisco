package adapter;

import gen.data.format.DataFormat;

public class AdapterTDSQL extends AdapterMySQL {
  /**
   * 如果需要用到涉及 dataFormat 的方法，那么必须设置，否则可以设为 null
   *
   * @param dataFormat dataFormat对象
   */
  public AdapterTDSQL(DataFormat dataFormat) {
    super(dataFormat);
  }
}
