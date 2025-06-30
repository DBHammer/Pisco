package config.replay;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import lombok.Data;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;
import replay.controller.util.ReduceMutationType;

@Data
public class ReplayConfig {

  private boolean keepRollback = false;
  private boolean serial = false;
  private boolean anaOnly = false;

  private boolean limitedEndValid = true;

  private boolean flatten = false;

  private boolean simpleFilter = false;

  private boolean cascadeDeleteValid = true;

  private List<String> operationSequence;

  private int sequenceBatchNumber = 1;

  private int loopNumber = 1;

  private List<ReduceMutationType> reduceMutationType = new ArrayList<>();

  private boolean skipEmptySelect = false;

  private boolean naiveSort = false;

  private String buggyOpId = null;

  public List<List<String>> parseOperationSequence() {
    if (operationSequence == null) return null;

    List<List<String>> sequence = new ArrayList<>();
    for (String op : operationSequence) {
      List<String> ops = new ArrayList<>();
      ops.add(op);
      sequence.add(ops);
    }
    return sequence;
  }

  public static ReplayConfig parse(String configPath) throws IOException {
    Yaml yaml = new Yaml(new Constructor(ReplayConfig.class));
    File replay = new File(configPath);
    if (replay.exists()) {
      InputStream is = Files.newInputStream(Paths.get(configPath));
      ReplayConfig replayConfig = yaml.load(is);
      is.close();
      return replayConfig;
    } else {
      return null;
    }
  }
}
