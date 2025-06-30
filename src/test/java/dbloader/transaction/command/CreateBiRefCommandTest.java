package dbloader.transaction.command;

import gen.shadow.Record;
import gen.shadow.RefInfo;
import org.junit.Test;

public class CreateBiRefCommandTest {

  @Test
  public void testSingle() {
    RefInfo refInfo = new RefInfo();
    Record recordSrc = new Record(0, 0, null, refInfo);
    Record recordDest = new Record(0, 1, null, refInfo);
    CreateBiRefCommand command = new CreateBiRefCommand(recordSrc, recordDest, "0", "1", null);
    command.execute();
    command.undo();
  }

  @Test
  public void testBi() {
    RefInfo refInfoSrc = new RefInfo();
    RefInfo refInfoDest = new RefInfo();
    Record recordSrc = new Record(0, 0, null, refInfoSrc);
    Record recordDest = new Record(0, 1, null, refInfoDest);
    CreateBiRefCommand command = new CreateBiRefCommand(recordSrc, recordDest, "0", "1", null);
    command.execute();
    command.undo();
  }
}
