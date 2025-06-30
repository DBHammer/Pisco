package dbloader.transaction.command;

public interface Command {
  void execute();

  void undo();
}
