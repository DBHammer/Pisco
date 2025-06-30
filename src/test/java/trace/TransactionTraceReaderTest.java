package trace;

import com.google.gson.Gson;
import java.io.IOException;
import org.junit.Test;

public class TransactionTraceReaderTest {
  @Test
  public void test() throws IOException {
    TransactionTraceReader reader =
        new TransactionTraceReader("./loadout/trace/iter-0-loader-0.json");
    reader.begin();
    while (reader.hasNext()) {
      OperationTrace operationTrace = reader.readOperationTrace();
      System.out.println(new Gson().toJson(operationTrace));
    }
    reader.end();
  }
}
