package visual;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import main.Main;

public class Visual {
  public static final String operationTracePythonFile = "./py/operationTrace.py";

  public static final String versionChainPythonFile = "./py/versionChain.py";

  public static final String validStatisticPythonFile = "./py/validStatistic.py";

  public static final String graphPythonFile = "./py/DiGraph.py";

  /**
   * version ./ana_out/debug.json ./ana_out/visOutput.html operation ./ana_out/debug.json
   * ./ana_out/visOutput.html valid ./out/stat dvsd graph ./ana_out/debug.json
   * ./ana_out/visOutput.html
   *
   * @param args
   */
  public static void main(String[] args) {
    if (args.length != 3) {
      Main.printUsageAndExit();
    }

    String pythonFile = null;
    switch (args[0]) {
      case "version":
        pythonFile = versionChainPythonFile;
        break;
      case "operation":
        pythonFile = operationTracePythonFile;
        break;
      case "valid":
        pythonFile = validStatisticPythonFile;
        break;
      case "graph":
        pythonFile = graphPythonFile;
        break;
    }

    String jsonFile = args[1];
    String htmlFile = args[2];

    try {
      String[] pyargs = new String[] {"python", pythonFile, jsonFile, htmlFile};
      Process proc = Runtime.getRuntime().exec(pyargs);

      BufferedReader in = new BufferedReader(new InputStreamReader(proc.getInputStream()));

      BufferedReader error = new BufferedReader(new InputStreamReader(proc.getErrorStream()));

      String line;
      System.out.println(in.readLine());
      while ((line = in.readLine()) != null) {
        System.out.println(line);
      }
      in.close();

      String err;
      while ((err = error.readLine()) != null) {
        System.out.println(err);
      }
      error.close();

      proc.waitFor();
    } catch (IOException | InterruptedException e) {
      System.out.println(e.getMessage());
    }
  }
}
