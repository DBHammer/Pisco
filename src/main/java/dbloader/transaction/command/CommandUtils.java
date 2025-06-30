package dbloader.transaction.command;

import com.google.common.collect.Lists;
import java.util.List;

public class CommandUtils {
  public static void executeCommandList(List<Command> commandList) {
    for (Command command : commandList) {
      command.execute();
    }
  }

  public static void undoCommandList(List<Command> commandList) {
    // 需要反向操作，尤其是加锁，解锁命令的undo顺序
    for (Command command : Lists.reverse(commandList)) {
      command.undo();
    }
  }
}
