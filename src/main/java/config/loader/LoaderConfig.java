package config.loader;

import config.schema.DistributionType;
import java.io.*;
import java.net.MalformedURLException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;
import lombok.Data;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;
import trace.IsolationLevel;

@Data
public class LoaderConfig {

  private boolean enableSchemaCustomization;
  // 是否使用load data local infile进行初始数据导入（需要服务器开启此功能）
  private boolean useLoadInfile;

  // transactionLoader 的数目，即线程数目，因为每个loader会创建一个线程
  private int numberOfLoader;

  // 每一轮会生成Loader加载事务，iter表示进行多少轮
  private int iter = 1;

  // 每次loader执行多少次事务之后退出
  private int execCountPerLoader;
  // 每次loader执行多长时间后终止(秒)，仅在execCountPerLoader为0时生效
  private long execTimePerLoader;

  // 事务模板数量
  private int numberOfTransactionCase;

  // 每条SQL执行超时时间以及时间单位
  private int queryTimeout;
  private TimeUnit queryTimeoutUnit;

  private HashMap<IsolationLevel, Integer> isolation;

  // 是否允许insert与delete互相转换
  private boolean autoTransform;

  // 是否为每个事务切换隔离级别
  private boolean randIsolationPerTransaction = true;

  // 访问分布
  private DistributionType visitDistribution;

  // 访问分布参数
  private String distributionParams;

  // 是否对Select启用访问分布
  private boolean enableForSelect;

  // 是否清除Trace中的debug信息
  private boolean clearDebugInfo = false;

  // between的参数间距
  private int betweenLength = 10;

  // 串行执行？
  private boolean serial;

  // 每次增删改查之后添加SavePoint的概率(%)
  private int probOfSavepoint;

  // rotate ?
  private boolean rotate;

  // dump数据库
  private boolean dump;

  // write trace?
  private boolean trace;

  private int slice;

  public static LoaderConfig parse(String configPath) throws IOException, DocumentException {
    if (configPath.endsWith("xml")) {
      return parseXML(configPath);
    } else if (configPath.endsWith("yml") || configPath.endsWith("yaml")) {
      return parseYAML(configPath);
    } else {
      throw new RuntimeException("不支持的文件格式");
    }
  }

  private static LoaderConfig parseYAML(String configPath) throws IOException {
    Yaml yaml = new Yaml(new Constructor(LoaderConfig.class));
    InputStream is = Files.newInputStream(Paths.get(configPath));
    return yaml.load(is);
  }

  private static LoaderConfig parseXML(String configPath)
      throws MalformedURLException, DocumentException {
    SAXReader reader = new SAXReader();
    Document document = reader.read(new File(configPath));
    Element rootElement = document.getRootElement();

    // 创建配置
    LoaderConfig config = new LoaderConfig();

    // 解析配置
    config.setNumberOfLoader(
        Integer.parseInt(rootElement.selectSingleNode("number_of_loader").getText()));
    config.setExecCountPerLoader(
        Integer.parseInt(rootElement.selectSingleNode("exec_count_per_loader").getText()));

    return config;
  }
}
