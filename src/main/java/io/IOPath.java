package io;

import java.io.File;
import java.io.IOException;
import org.apache.commons.io.FileUtils;

public class IOPath {

  public final String configDir;
  public final String outputDir;

  public final String formatConfigPath;
  public final String schemaConfigPath;
  public final String operationConfigPath;
  public final String loaderConfigPath;
  public final String dataGenConfigPath;
  public final String replayConfigPath;

  public final String traceDir;
  public final String transactionTemplateDir;
  public final String initialDataDir;
  public final String schemaDir;
  public final String dumpDir;

  public final String initialSQLDest;
  public final String initialCSVDest;
  public final String schemaSqlDest;
  public final String objectCollectionDest;
  public final String c3p0ConfigPath;
  public final String consistentPath;

  public IOPath(String configDir, String outputDir) {
    super();
    this.configDir = configDir;
    this.outputDir = outputDir;

    // 配置文件路径
    formatConfigPath = String.format("%s/format.xml", configDir);
    schemaConfigPath = String.format("%s/schema.yml", configDir);
    operationConfigPath = String.format("%s/operation.yml", configDir);
    loaderConfigPath = String.format("%s/loader.yml", configDir);
    dataGenConfigPath = String.format("%s/data_generation.yml", configDir);
    c3p0ConfigPath = String.format("%s/c3p0.xml", configDir);
    replayConfigPath = String.format("%s/replay.yml", configDir);
    consistentPath = String.format("%s/consistent.yml", configDir);

    // 几个输出文件夹
    traceDir = String.format("%s/trace", outputDir);
    transactionTemplateDir = String.format("%s/transaction_template", outputDir);
    initialDataDir = String.format("%s/init_data", outputDir);
    schemaDir = String.format("%s/schema", outputDir);
    dumpDir = String.format("%s/dump", outputDir);

    // 输出目标文件
    initialSQLDest = String.format("%s/dataInsert.sql", initialDataDir);
    initialCSVDest = String.format("%s/dataInsert.csv", initialDataDir);
    schemaSqlDest = String.format("%s/schema.sql", schemaDir);
    objectCollectionDest = String.format("%s/ObjectCollection.obj", outputDir);
  }

  public void cleanForLoad() throws IOException {

    String[] outSubDirs = {
      traceDir, transactionTemplateDir, initialDataDir, schemaDir,
    };

    File outRootDirFile = new File(outputDir);
    outRootDirFile.mkdir();

    // 不能删除out文件夹，因为log会输出到out/orca.log，这里如果删除，日志文件也会被删除
    // 但是理论上来说，log4j会在此之前创建日志文件，这样一旦删除就没有日志文件了

    for (String subDir : outSubDirs) {
      File outDirFile = new File(subDir);

      // 清空子文件夹
      FileUtils.deleteDirectory(outDirFile);
      outDirFile.mkdirs();
    }
  }

  public void cleanForReload() throws IOException {

    String[] outSubDirs = {traceDir};

    File outRootDirFile = new File(outputDir);
    outRootDirFile.mkdir();

    for (String subDir : outSubDirs) {
      File outDirFile = new File(subDir);

      // 清空子文件夹
      FileUtils.deleteDirectory(outDirFile);
      outDirFile.mkdirs();
    }
  }

  public void copyTraceDir() throws IOException {
    String storeDir = String.format("%s/store", outputDir);
    File trace = new File(traceDir);
    File store = new File(storeDir);

    if (store.exists()) {
      return;
    }

    FileUtils.copyDirectory(trace, store);
  }

  public void copyDir(String sourceDir, String destDir) throws IOException {
    File source = new File(sourceDir);
    File dest = new File(destDir);
    FileUtils.copyDirectory(source, dest);
  }
}
