package main;

import ana.main.OrcaVerify;
import java.io.IOException;
import java.sql.SQLException;
import replay.main.OrcaReplay;

public class Main {
  public static void main(String[] args) throws InterruptedException, SQLException, IOException {
    if (args.length < 1) {
      printUsageAndExit();
    }
    String subProgram = args[0];
    if ("replay".equals(subProgram)) {
      OrcaReplay.main(args);
    } else {
      printUsageAndExit();
    }
  }

  public static void printUsageAndExit() {
    System.err.println("Usage: java -jar $jarfile [replay] ...");
    System.err.println();
    System.err.println("Other parameters are described as follows.");
    System.err.println("For replay : configDir originOutputDir newOutputDir");
    System.exit(-1);
  }
}
