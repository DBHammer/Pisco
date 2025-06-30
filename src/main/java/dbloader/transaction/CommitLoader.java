package dbloader.transaction;

import dbloader.transaction.command.Command;
import dbloader.transaction.command.CommandUtils;
import dbloader.transaction.command.LockCommand;
import dbloader.transaction.command.UnlockCommand;
import dbloader.transaction.savepoint.OrcaSavePoint;
import exception.CommitException;
import gen.shadow.Record;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Set;
import trace.OperationTrace;

public class CommitLoader {
  public static void loadCommitOperation(
      TransactionLoader loader,
      Connection conn,
      OperationTrace operationTrace,
      Set<Record> needLockRecordSet,
      List<Command> commandList)
      throws CommitException {
    // 添加 解锁 和 加锁命令
    Command lockCommand = new LockCommand(needLockRecordSet);
    Command unlockCommand = new UnlockCommand(needLockRecordSet);
    // 执行命令，更新MiniShadow
    // 这里有一点是确定的
    // 只要能获取所有的锁，必然能成功更新MiniShadow
    // 如果获取锁失败了，那么MiniShadow不会被做任何修改
    // 直接报错回滚不会影响MiniShadow和数据库状态的一致性
    // 但是假如COMMIT过程出现了异常，必然会导致MiniShadow与数据库状态不一致
    // 这时候，也许可以通过 undo 命令 来维持一致性
    // 不过，一旦释放了锁，再次获取不一定会成功
    // 假如COMMIT失败，又必须能够获取所有的锁
    // 要么修改unlock命令的undo，死等，获取所有锁
    // 要么就是先执行所有修改，但不释放锁
    // 在COMMIT成功之后再释放锁
    // 若COMMIT失败，由于持有所有的锁，必然能成功执行所有命令的undo操作
    // 上锁
    //    lockCommand.execute(); 这里会和数据库的锁协同形成死锁，选择不加锁，牺牲minishadow的有效性保证吞吐

    // 操作生成时间戳
    operationTrace.setStartTime(System.nanoTime());

    // 暂不解锁
    try {
      // 提交事务
      conn.commit();
      // 执行对MiniShadow的修改
      CommandUtils.executeCommandList(commandList);

    } catch (SQLException e) {
      throw new CommitException("COMMIT 执行失败", e);
    } finally {
      // 只有成功加锁，才会走到这里
      // 只要成功加锁，就必须要执行解锁命令
      // 解锁
      //      unlockCommand.execute();
    }
  }

  public static void loadCommitOperation(
      TransactionLoader loader,
      Connection conn,
      OperationTrace operationTrace,
      OrcaSavePoint orcaSavePoint)
      throws CommitException, SQLException {
    conn.rollback(orcaSavePoint.getSavepoint());
    CommitLoader.loadCommitOperation(
        loader,
        conn,
        operationTrace,
        orcaSavePoint.getNeedLockRecordSet(),
        orcaSavePoint.getCommandList());
    operationTrace.setSavepoint(orcaSavePoint.getSavepoint().getSavepointName());
  }
}
