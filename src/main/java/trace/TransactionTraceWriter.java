package trace;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.stream.JsonWriter;
import context.OrcaContext;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import org.apache.commons.io.FileUtils;
import symbol.Symbol;

public class TransactionTraceWriter {
  private JsonWriter writer;
  private final Gson gson;
  private final boolean trace;
  private int fileno = 0;
  private int traceno = 0;
  private final String dest;
  private String currentDest;

  public TransactionTraceWriter(String dest, boolean trace) throws IOException {
    super();
    gson = new GsonBuilder().disableHtmlEscaping().create();
    this.trace = trace;
    this.dest = dest;
    this.begin();
  }

  private void begin() throws IOException {
    currentDest = String.format("%s.%s.%s", dest, fileno, Symbol.TRACE_EXTENSION);
    writer = new JsonWriter(new BufferedWriter(new FileWriter(currentDest + ".writing")));
    writer.setIndent("    ");
    writer.beginArray();
    fileno++;
  }

  private void end() throws IOException {
    writer.endArray();
  }

  public void writeOperationTrace(OperationTrace operationTrace) throws IOException {
    if (trace) {
      if (OrcaContext.configColl.getLoader().isClearDebugInfo()) {
        operationTrace.clearDebugInfo();
      }
      if (operationTrace.getOperationTraceType() == OperationTraceType.START
          || operationTrace.getOperationTraceType() == OperationTraceType.DDL
          || operationTrace.getOperationTraceType() == OperationTraceType.FAULT) {
        if (traceno % OrcaContext.configColl.getLoader().getSlice() == 0) {
          this.close();
          this.begin();
        }
        traceno++;
      }

      operationTrace.setTraceFile(currentDest);
      gson.toJson(operationTrace, OperationTrace.class, writer);
      writer.flush();
    }
  }

  public void close() throws IOException {
    if (writer != null) {
      this.end();
      writer.close();
      writer = null;
      FileUtils.moveFile(new File(currentDest + ".writing"), new File(currentDest));
    }
  }
}
