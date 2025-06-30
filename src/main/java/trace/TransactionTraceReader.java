package trace;

import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import symbol.Symbol;

/** 流式读取 OperationTrace 先begin，然后不断调用read，最后end */
public class TransactionTraceReader {
  private JsonReader reader;
  private final Gson gson;
  private int fileno = 0;
  private final int traceno = 0;
  private final String src;
  private String currentSrc;

  public TransactionTraceReader(String src) {
    super();
    this.gson = new Gson();
    this.src = src;
  }

  public void begin() throws IOException {
    currentSrc = String.format("%s.%s.%s", src, fileno, Symbol.TRACE_EXTENSION);
    File newFile = new File(currentSrc + ".writing");
    while (newFile.exists()) {}
    if (new File(currentSrc).exists()) {
      this.reader = new JsonReader(new BufferedReader(new FileReader(currentSrc)));
      reader.beginArray();
    }
  }

  public boolean hasNext() throws IOException {
    if (reader == null) {
      return false;
    }
    if (reader.hasNext()) {
      return true;
    }
    fileno++;
    currentSrc = String.format("%s.%s.%s", src, fileno, Symbol.TRACE_EXTENSION);
    this.end();
    this.begin();
    return reader != null && reader.hasNext();
  }

  public OperationTrace readOperationTrace() {
    return gson.fromJson(reader, OperationTrace.class);
  }

  public boolean isNull() {
    return reader == null;
  }

  public void end() throws IOException {
    if (reader == null) {
      return;
    }
    reader.endArray();
    reader.close();
    reader = null;
  }
}
