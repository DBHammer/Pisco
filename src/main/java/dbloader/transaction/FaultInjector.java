package dbloader.transaction;

import context.OrcaContext;
import dbloader.transaction.command.Command;
import gen.operation.basic.OperationFault;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.util.LinkedList;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import trace.OperationTrace;
import trace.OperationTraceType;
import util.rand.RandUtil;

public class FaultInjector extends OperationLoader {
  public static final Logger logger = LogManager.getLogger(FaultInjector.class);

  /**
   * 执行update操作
   *
   * @param operation 本次要执行的操作
   * @param conn 数据库连接
   * @param operationTrace 要将trace记录到这个对象中
   * @return 命令列表
   */
  public static synchronized List<Command> injectFault(
      OperationFault operation, Connection conn, OperationTrace operationTrace) {

    List<Command> commandList = new LinkedList<>();
    RandUtil randUtil = new RandUtil();
    String sqlInstance = "";

    operationTrace.setOperationTraceType(OperationTraceType.FAULT);

    String toolDir = OrcaContext.configColl.getFault().getToolDir();

    // 如果chaos blade没有在正确的位置上，提前终止
    if (!checkChaosBlade(toolDir)) {
      logger.warn("Fault injection tool not found! Check config.");
      return commandList;
    }

    String networkConfig =
        String.format(
            "--destination-ip %s "
                + "--interface %s "
                + "--local-port %s "
                + "--remote-port %s "
                + "--timeout %s",
            OrcaContext.configColl.getFault().getDstIp(), // 目标 IP
            OrcaContext.configColl.getFault().getNetworkDevice(),
            OrcaContext.configColl.getFault().getLocalPort(), // 本地端口
            OrcaContext.configColl.getFault().getRemotePort(), // 远程端口
            OrcaContext.configColl.getFault().getTimeout() // 超时时间
            );

    switch (operation.getFaultType()) {
      case DiskBurn:
        {
          /**
           * case: blade create disk burn --read --write --path / --path string 指定提升磁盘 io
           * 的目录，会作用于其所在的磁盘上，默认值是 / --read 触发提升磁盘读 IO 负载，会创建 600M 的文件用于读，销毁实验会自动删除 --size string
           * 块大小, 单位是 M, 默认值是 10，一般不需要修改，除非想更大的提高 io 负载 --timeout string 设定运行时长，单位是秒，通用参数 --write
           * 触发提升磁盘写 IO 负载，会根据块大小的值来写入一个文件，比如块大小是 10，则固定的块的数量是 100，则会创建 1000M 的文件，销毁实验会自动删除
           */
          sqlInstance =
              String.format(
                  "create disk burn " + "--timeout %d " + "--read --write " + "--path %s",
                  OrcaContext.configColl.getFault().getTimeout(),
                  OrcaContext.configColl.getFault().getStoragePath());
          break;
        }
      case DiskFill:
        {
          /**
           * case: blade create disk fill --path /home --size 40000 --path string 需要填充的目录，默认值是 /
           * --size string 需要填充的文件大小，单位是 M，取值是整数，例如 --size 1024 --reserve string
           * 保留磁盘大小，单位是MB。取值是不包含单位的正整数，例如 --reserve 1024。如果 size、percent、reserve 参数都存在，优先级是 percent
           * > reserve > size --percent string 指定磁盘使用率，取值是不带%号的正整数，例如 --percent 80 --retain-handle
           * 是否保留填充 --timeout string 设定运行时长，单位是秒，通用参数
           */
          sqlInstance =
              String.format(
                  "create disk fill " + "--timeout %d " + "--path %s " + "--percent %d",
                  OrcaContext.configColl.getFault().getTimeout(),
                  OrcaContext.configColl.getFault().getStoragePath(),
                  randUtil.nextInt(20) + 80);
          break;
        }
      case TimeTravel:
        {
          /** case: blade create time travel --offset 5m30s */
          sqlInstance =
              String.format(
                  "create time travel " + "--timeout %d " + "--offset %ss ",
                  OrcaContext.configColl.getFault().getTimeout(),
                  OrcaContext.configColl.getFault().getDelayTime(),
                  randUtil.nextInt(20) + 80);
          break;
        }
      case NetworkLoss:
        {
          /**
           * case: blade create network loss --percent 70 --interface eth0 --local-port 8080,8081
           * --destination-ip string 目标 IP. 支持通过子网掩码来指定一个网段的IP地址, 例如 192.168.1.0/24. 则
           * 192.168.1.0~192.168.1.255 都生效。你也可以指定固定的 IP，如 192.168.1.1 或者
           * 192.168.1.1/32，也可以通过都号分隔多个参数，例如 192.168.1.1,192.168.2.1。 --exclude-port string
           * 排除掉的端口，默认会忽略掉通信的对端端口，目的是保留通信可用。可以指定多个，使用逗号分隔或者连接符表示范围，例如 22,8000 或者 8000-8010。 这个参数不能与
           * --local-port 或者 --remote-port 参数一起使用 --exclude-ip string 排除受影响的
           * IP，支持通过子网掩码来指定一个网段的IP地址, 例如 192.168.1.0/24. 则 192.168.1.0~192.168.1.255 都生效。你也可以指定固定的
           * IP，如 192.168.1.1 或者 192.168.1.1/32，也可以通过都号分隔多个参数，例如 192.168.1.1,192.168.2.1。
           * --interface string 网卡设备，例如 eth0 (必要参数) --local-port string
           * 本地端口，一般是本机暴露服务的端口。可以指定多个，使用逗号分隔或者连接符表示范围，例如 80,8000-8080 --percent string 丢包百分比，取值在[0,
           * 100]的正整数 (必要参数) --remote-port string 远程端口，一般是要访问的外部暴露服务的端口。可以指定多个，使用逗号分隔或者连接符表示范围，例如
           * 80,8000-8080 --force 强制覆盖已有的 tc 规则，请务必在明确之前的规则可覆盖的情况下使用 --ignore-peer-port 针对添加
           * --exclude-port 参数，报 ss 命令找不到的情况下使用，忽略排除端口 --timeout string 设定运行时长，单位是秒，通用参数
           */
          sqlInstance =
              String.format(
                  "create network loss " + "--percent %d " + networkConfig, randUtil.nextInt(100));
          break;
        }
      case ProcessKill:
        {
          /**
           * case: blade create process kill --process SimpleHTTPServer --process string
           * 进程关键词，会在整个命令行中查找 --process-cmd string 进程命令，只会在命令中查找 --count string 限制杀掉进程的数量，0 表示无限制
           * --signal string 指定杀进程的信号量，默认是 9，例如 --signal 15 --timeout string 设定运行时长，单位是秒，通用参数
           */
          sqlInstance =
              String.format(
                  "create process kill " + "--process-cmd %s " + "--count 2 " + "--signal %d",
                  OrcaContext.configColl.getFault().getProcessName(),
                  OrcaContext.configColl.getFault().getKillSignal());
          break;
        }
      case ProcessStop:
        {
          /**
           * case: blade create process stop --process SimpleHTTPServer --process string
           * 进程关键词，会在整个命令行中查找 --process-cmd string 进程命令，只会在命令中查找 --timeout string 设定运行时长，单位是秒，通用参数
           */
          sqlInstance =
              String.format(
                  "create process stop " + "--process-cmd %s " + "--count 2 " + "--timeout %d",
                  OrcaContext.configColl.getFault().getProcessName(),
                  OrcaContext.configColl.getFault().getTimeout());
          break;
        }
      case NetworkDelay:
        {
          /**
           * case: blade create network delay --time 3000 --offset 1000 --interface eth0
           * --local-port 8080,8081 --destination-ip string 目标 IP. 支持通过子网掩码来指定一个网段的IP地址, 例如
           * 192.168.1.0/24. 则 192.168.1.0~192.168.1.255 都生效。你也可以指定固定的 IP，如 192.168.1.1 或者
           * 192.168.1.1/32，也可以通过都号分隔多个参数，例如 192.168.1.1,192.168.2.1。 --exclude-port string
           * 排除掉的端口，默认会忽略掉通信的对端端口，目的是保留通信可用。可以指定多个，使用逗号分隔或者连接符表示范围，例如 22,8000 或者 8000-8010。 这个参数不能与
           * --local-port 或者 --remote-port 参数一起使用 --exclude-ip string 排除受影响的
           * IP，支持通过子网掩码来指定一个网段的IP地址, 例如 192.168.1.0/24. 则 192.168.1.0~192.168.1.255 都生效。你也可以指定固定的
           * IP，如 192.168.1.1 或者 192.168.1.1/32，也可以通过都号分隔多个参数，例如 192.168.1.1,192.168.2.1。
           * --interface string 网卡设备，例如 eth0 (必要参数) --local-port string
           * 本地端口，一般是本机暴露服务的端口。可以指定多个，使用逗号分隔或者连接符表示范围，例如 80,8000-8080 --offset string 延迟时间上下浮动的值,
           * 单位是毫秒 --remote-port string 远程端口，一般是要访问的外部暴露服务的端口。可以指定多个，使用逗号分隔或者连接符表示范围，例如 80,8000-8080
           * --time string 延迟时间，单位是毫秒 (必要参数) --force 强制覆盖已有的 tc 规则，请务必在明确之前的规则可覆盖的情况下使用
           * --ignore-peer-port 针对添加 --exclude-port 参数，报 ss 命令找不到的情况下使用，忽略排除端口 --timeout string
           * 设定运行时长，单位是秒，通用参数
           */
          sqlInstance =
              String.format(
                  "create network delay " + "--time %d " + networkConfig,
                  OrcaContext.configColl.getFault().getDelayTime());
          break;
        }
      case NetworkOrder:
        {
          /**
           * case: blade c network reorder --correlation 80 --percent 50 --gap 2 --time 500
           * --interface eth0 --destination-ip 180.101.49.12 --destination-ip string 目标 IP.
           * 支持通过子网掩码来指定一个网段的IP地址, 例如 192.168.1.0/24. 则 192.168.1.0~192.168.1.255 都生效。你也可以指定固定的 IP，如
           * 192.168.1.1 或者 192.168.1.1/32，也可以通过都号分隔多个参数，例如 192.168.1.1,192.168.2.1。 --exclude-port
           * string 排除掉的端口，默认会忽略掉通信的对端端口，目的是保留通信可用。可以指定多个，使用逗号分隔或者连接符表示范围，例如 22,8000 或者 8000-8010。
           * 这个参数不能与 --local-port 或者 --remote-port 参数一起使用 --exclude-ip string 排除受影响的
           * IP，支持通过子网掩码来指定一个网段的IP地址, 例如 192.168.1.0/24. 则 192.168.1.0~192.168.1.255 都生效。你也可以指定固定的
           * IP，如 192.168.1.1 或者 192.168.1.1/32，也可以通过都号分隔多个参数，例如 192.168.1.1,192.168.2.1。
           * --interface string 网卡设备，例如 eth0 (必要参数) --local-port string
           * 本地端口，一般是本机暴露服务的端口。可以指定多个，使用逗号分隔或者连接符表示范围，例如 80,8000-8080 --offset string 延迟时间上下浮动的值,
           * 单位是毫秒 --remote-port string 远程端口，一般是要访问的外部暴露服务的端口。可以指定多个，使用逗号分隔或者连接符表示范围，例如 80,8000-8080
           * --correlation string 和上一包的相关性，取值在 0~100，必要参数，例如 --correlation 70 --gap string
           * 包序列大小，取值是正整数，例如 --gap 5 --percent string 立即发送百分比，取值是不带%号的正整数，例如 --percent 50，(必要参数)
           * --time string 网络包延迟时间，单位是毫秒，默认值是 10，取值时正整数 --force 强制覆盖已有的 tc 规则，请务必在明确之前的规则可覆盖的情况下使用
           * --ignore-peer-port 针对添加 --exclude-port 参数，报 ss 命令找不到的情况下使用，忽略排除端口 --timeout string
           * 设定运行时长，单位是秒，通用参数
           */
          sqlInstance =
              String.format(
                  "create network reorder "
                      + "--percent %d "
                      + "--correlation %d "
                      + "--gap %d "
                      + networkConfig,
                  randUtil.nextInt(100),
                  randUtil.nextInt(100),
                  randUtil.nextInt());
          break;
        }
      case NetworkCorrupt:
        {
          /**
           * case: blade create network corrupt --percent 80 --destination-ip 180.101.49.12
           * --interface eth0 --destination-ip string 目标 IP. 支持通过子网掩码来指定一个网段的IP地址, 例如
           * 192.168.1.0/24. 则 192.168.1.0~192.168.1.255 都生效。你也可以指定固定的 IP，如 192.168.1.1 或者
           * 192.168.1.1/32，也可以通过都号分隔多个参数，例如 192.168.1.1,192.168.2.1。 --exclude-port string
           * 排除掉的端口，默认会忽略掉通信的对端端口，目的是保留通信可用。可以指定多个，使用逗号分隔或者连接符表示范围，例如 22,8000 或者 8000-8010。 这个参数不能与
           * --local-port 或者 --remote-port 参数一起使用 --exclude-ip string 排除受影响的
           * IP，支持通过子网掩码来指定一个网段的IP地址, 例如 192.168.1.0/24. 则 192.168.1.0~192.168.1.255 都生效。你也可以指定固定的
           * IP，如 192.168.1.1 或者 192.168.1.1/32，也可以通过都号分隔多个参数，例如 192.168.1.1,192.168.2.1。
           * --interface string 网卡设备，例如 eth0 (必要参数) --local-port string
           * 本地端口，一般是本机暴露服务的端口。可以指定多个，使用逗号分隔或者连接符表示范围，例如 80,8000-8080 --offset string 延迟时间上下浮动的值,
           * 单位是毫秒 --remote-port string 远程端口，一般是要访问的外部暴露服务的端口。可以指定多个，使用逗号分隔或者连接符表示范围，例如 80,8000-8080
           * --percent 包损坏百分比，取值是不带%号的正整数 --force 强制覆盖已有的 tc 规则，请务必在明确之前的规则可覆盖的情况下使用
           * --ignore-peer-port 针对添加 --exclude-port 参数，报 ss 命令找不到的情况下使用，忽略排除端口 --timeout string
           * 设定运行时长，单位是秒，通用参数
           */
          sqlInstance =
              String.format(
                  "create network corrupt " + "--percent %d " + networkConfig,
                  randUtil.nextInt(100));
          break;
        }
      case NetworkDuplicate:
        {
          /**
           * case: blade create network duplicate --percent 80 --destination-ip 180.101.49.12
           * --interface eth0 --destination-ip string 目标 IP. 支持通过子网掩码来指定一个网段的IP地址, 例如
           * 192.168.1.0/24. 则 192.168.1.0~192.168.1.255 都生效。你也可以指定固定的 IP，如 192.168.1.1 或者
           * 192.168.1.1/32，也可以通过都号分隔多个参数，例如 192.168.1.1,192.168.2.1。 --exclude-port string
           * 排除掉的端口，默认会忽略掉通信的对端端口，目的是保留通信可用。可以指定多个，使用逗号分隔或者连接符表示范围，例如 22,8000 或者 8000-8010。 这个参数不能与
           * --local-port 或者 --remote-port 参数一起使用 --exclude-ip string 排除受影响的
           * IP，支持通过子网掩码来指定一个网段的IP地址, 例如 192.168.1.0/24. 则 192.168.1.0~192.168.1.255 都生效。你也可以指定固定的
           * IP，如 192.168.1.1 或者 192.168.1.1/32，也可以通过都号分隔多个参数，例如 192.168.1.1,192.168.2.1。
           * --interface string 网卡设备，例如 eth0 (必要参数) --local-port string
           * 本地端口，一般是本机暴露服务的端口。可以指定多个，使用逗号分隔或者连接符表示范围，例如 80,8000-8080 --offset string 延迟时间上下浮动的值,
           * 单位是毫秒 --remote-port string 远程端口，一般是要访问的外部暴露服务的端口。可以指定多个，使用逗号分隔或者连接符表示范围，例如 80,8000-8080
           * --percent 包损坏百分比，取值是不带%号的正整数 --force 强制覆盖已有的 tc 规则，请务必在明确之前的规则可覆盖的情况下使用
           * --ignore-peer-port 针对添加 --exclude-port 参数，报 ss 命令找不到的情况下使用，忽略排除端口 --timeout string
           * 设定运行时长，单位是秒，通用参数
           */
          sqlInstance =
              String.format(
                  "create network duplicate " + "--percent %d " + networkConfig,
                  randUtil.nextInt(100));
          break;
        }
      default:
        break;
    }
    sqlInstance = "sudo " + toolDir + " " + sqlInstance;
    operationTrace.setSql(sqlInstance);
    logger.info(
        "Operation ID :{} ; SQL : {}", operationTrace.getOperationID(), operationTrace.getSql());

    operationTrace.setStartTime(System.nanoTime());
    try {
      //            stat.execute(sqlInstance);
      inject(sqlInstance);
    } catch (Exception e) {
      logger.warn(e.getMessage());
    }
    operationTrace.setFinishTime(System.nanoTime());

    return commandList;
  }

  private static boolean checkChaosBlade(String toolDir) {
    return new File(toolDir).exists();
  }

  private static void inject(final String faultCommand) {
    try {
      Runtime.getRuntime().exec(faultCommand);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static void printResults(Process process) throws IOException {
    BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
    String line = "";
    while ((line = reader.readLine()) != null) {
      System.out.println(line);
    }
  }
}
