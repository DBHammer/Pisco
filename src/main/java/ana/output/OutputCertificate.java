package ana.output;

import ana.graph.Dependency;
import ana.graph.DependencyGraph;
import ana.main.Config;
import ana.version.HistoryVersion;
import ana.version.Version;
import ana.version.VersionChain;
import ana.window.WriteSet;
import ana.window.profile.Profile;
import com.google.gson.Gson;
import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jgrapht.graph.SimpleDirectedGraph;
import replay.controller.ReplayController;
import trace.OperationTrace;
import trace.OperationTraceType;
import trace.TupleTrace;

public class OutputCertificate {

  private static final Logger logger = LogManager.getLogger(OutputCertificate.class);

  /** certificate 写io handler */
  private static BufferedWriter certificateHandler4RecordLock = null;

  private static BufferedWriter certificateHandler4PredicateLock = null;
  private static BufferedWriter certificateHandler4ReadSelf = null;
  private static BufferedWriter certificateHandler4ReadOther = null;
  private static BufferedWriter tempHandler4ReadOther = null;
  private static BufferedWriter certificateHandler4SerializeAccess = null;
  private static BufferedWriter certificateHandler4DependencyCycle = null;

  /** 将对象按json格式输出 */
  private static Gson gson = null;

  public static void initialize() {
    gson = new Gson();
  }

  public static void flush() {}

  static class OperationTraceJson {
    private final String description;

    private final Object object;

    public OperationTraceJson(String description, Object object) {
      this.description = description;
      this.object = object;
    }
  }

  public static void outputRecordLockError(
      OperationTrace operationTraceCertificate,
      OperationTrace terminalOperationTraceCertificate,
      OperationTrace nextOperationTraceCertificate,
      TupleTrace tupleTrace1,
      TupleTrace tupleTrace2) {
    ErrorStatistics.increase(
        ErrorStatistics.ErrorType.RECORD_LOCK_ERROR,
        nextOperationTraceCertificate.getTransactionID());
    ReplayController.addErrorTrace(nextOperationTraceCertificate);

    ArrayList<OperationTraceJson> certificateRecordLock = new ArrayList<>();

    try {
      certificateHandler4RecordLock =
          new BufferedWriter(
              new OutputStreamWriter(
                  new FileOutputStream(
                      Config.CERTIFICATE_OUTPUT_FILE
                          + "RecordLock"
                          + ErrorStatistics.errorStatistics
                              .get(ErrorStatistics.ErrorType.RECORD_LOCK_ERROR)
                              .size()
                          + ".json")));
    } catch (FileNotFoundException e) {
      logger.warn(e);
    }

    certificateRecordLock.add(new OperationTraceJson("record-level lock error", null));

    certificateRecordLock.add(new OperationTraceJson("record-level lock error", null));

    certificateRecordLock.add(
        new OperationTraceJson(
            "the operation(T1,Op) of holding lock: ", operationTraceCertificate));

    certificateRecordLock.add(
        new OperationTraceJson(
            "the parallel operation (T2,Op) without acquiring lock: ",
            nextOperationTraceCertificate));

    certificateRecordLock.add(
        new OperationTraceJson(
            "the commit or rollback of the operation(T1,C or R) to release lock: ",
            terminalOperationTraceCertificate));

    certificateRecordLock.add(
        new OperationTraceJson("the conflict tuple-level lock(R1) held by T1,Op: ", tupleTrace1));

    certificateRecordLock.add(
        new OperationTraceJson("the conflict tuple-level lock(R2) held by T2,Op: ", tupleTrace2));

    try {
      certificateHandler4RecordLock.write(gson.toJson(certificateRecordLock));
      certificateHandler4RecordLock.flush();
      certificateHandler4RecordLock.close();
    } catch (IOException e) {
      logger.warn(e);
    }
  }

  public static void outputPredicateLockError(
      OperationTrace operationTraceCertificate,
      OperationTrace terminalOperationTraceCertificate,
      OperationTrace nextOperationTraceCertificate,
      String predicateLock,
      TupleTrace tupleTrace) {
    ErrorStatistics.increase(
        ErrorStatistics.ErrorType.PREDICATE_LOCK_ERROR,
        nextOperationTraceCertificate.getTransactionID());

    ArrayList<OperationTraceJson> certificatePredicateLock = new ArrayList<>();

    try {
      certificateHandler4PredicateLock =
          new BufferedWriter(
              new OutputStreamWriter(
                  new FileOutputStream(
                      Config.CERTIFICATE_OUTPUT_FILE
                          + "PredicateLock"
                          + ErrorStatistics.errorStatistics
                              .get(ErrorStatistics.ErrorType.PREDICATE_LOCK_ERROR)
                              .size()
                          + ".json")));
    } catch (FileNotFoundException e) {
      logger.warn(e);
    }

    certificatePredicateLock.add(new OperationTraceJson("predicate lock error", null));

    certificatePredicateLock.add(
        new OperationTraceJson(
            "the operation(T1,Op) of holding lock: ", operationTraceCertificate));

    certificatePredicateLock.add(
        new OperationTraceJson(
            "the parallel operation (T2,Op) without acquiring the lock: ",
            nextOperationTraceCertificate));

    certificatePredicateLock.add(
        new OperationTraceJson(
            "the commit or rollback of the operation(T1,C or R) to release lock: ",
            terminalOperationTraceCertificate));

    certificatePredicateLock.add(
        new OperationTraceJson("the conflict predicate-level lock(P): ", predicateLock));

    certificatePredicateLock.add(
        new OperationTraceJson("the conflict tuple-level lock(R): ", tupleTrace));

    try {
      certificateHandler4PredicateLock.write(gson.toJson(certificatePredicateLock));
      certificateHandler4PredicateLock.flush();
      certificateHandler4PredicateLock.close();
    } catch (IOException e) {
      logger.warn(e);
    }
  }

  public static void outputReadSelfError(
      OperationTrace operationTrace, TupleTrace tupleTrace, HashMap<String, Version> writeSet) {

    if (operationTrace.getTransactionID().equals("0-0-11,0")) {
      System.out.println();
    }
    ErrorStatistics.increase(
        ErrorStatistics.ErrorType.READ_SELF_ERROR, operationTrace.getTransactionID());
    ReplayController.addErrorTrace(operationTrace);

    ArrayList<OperationTraceJson> certificateReadSelf = new ArrayList<>();

    try {
      certificateHandler4ReadSelf =
          new BufferedWriter(
              new OutputStreamWriter(
                  new FileOutputStream(
                      Config.CERTIFICATE_OUTPUT_FILE
                          + "ReadSelf"
                          + ErrorStatistics.errorStatistics
                              .get(ErrorStatistics.ErrorType.READ_SELF_ERROR)
                              .size()
                          + ".json")));
    } catch (FileNotFoundException e) {
      logger.warn(e);
    }

    certificateReadSelf.add(new OperationTraceJson("read self error", null));

    certificateReadSelf.add(
        new OperationTraceJson("the read operation(T1,Op) that presents error", operationTrace));

    certificateReadSelf.add(new OperationTraceJson("the error record(R) of T1,Op", tupleTrace));

    certificateReadSelf.add(new OperationTraceJson("the write set of T1 before T1,Op1", writeSet));

    certificateReadSelf.add(
        new OperationTraceJson(
            "the version chain(VC) on R: ", HistoryVersion.getVersionChain(tupleTrace.getKey())));

    try {
      certificateHandler4ReadSelf.write(gson.toJson(certificateReadSelf));
      certificateHandler4ReadSelf.flush();
      certificateHandler4ReadSelf.close();
    } catch (IOException e) {
      logger.warn(e);
    }
  }

  public static void outputReadOtherError(
      OperationTrace operationTrace, TupleTrace tupleTrace, ArrayList<Version> candidateReadSet) {
    ErrorStatistics.increase(
        ErrorStatistics.ErrorType.READ_OTHER_ERROR, operationTrace.getTransactionID());
    ReplayController.addErrorTrace(operationTrace);

    ArrayList<OperationTraceJson> certificateReadOther = new ArrayList<>();

    try {
      certificateHandler4ReadOther =
          new BufferedWriter(
              new OutputStreamWriter(
                  new FileOutputStream(
                      Config.CERTIFICATE_OUTPUT_FILE
                          + "ReadOther"
                          + ErrorStatistics.errorStatistics
                              .get(ErrorStatistics.ErrorType.READ_OTHER_ERROR)
                              .size()
                          + ".json")));
    } catch (FileNotFoundException e) {
      logger.warn(e);
    }
    operationTrace = new OperationTrace(operationTrace);
    if (operationTrace.getReadTupleList() != null) {
      operationTrace.setReadTupleList(null);
    }
    certificateReadOther.add(new OperationTraceJson("read other error", null));

    certificateReadOther.add(
        new OperationTraceJson("the read operation(T1,Op) that presents error", operationTrace));

    certificateReadOther.add(new OperationTraceJson("the error record(R) of T1,Op", tupleTrace));

    certificateReadOther.add(
        new OperationTraceJson(
            "the version chain(VC) on R: ", HistoryVersion.getVersionChain(tupleTrace.getKey())));

    certificateReadOther.add(
        new OperationTraceJson("the candidate read set(CRS) of T1,Op on R: ", candidateReadSet));

    certificateReadOther.add(
        new OperationTraceJson(
            "the write set of T1 before T1,Op1",
            WriteSet.getTransactionVersions(
                WriteSet.getReadConsistency(), operationTrace.getTransactionID())));

    try {

      certificateHandler4ReadOther.write(gson.toJson(certificateReadOther));
      certificateHandler4ReadOther.flush();
      certificateHandler4ReadOther.close();
    } catch (IOException e) {
      logger.warn(e);
    }

    //    try {
    //      tempHandler4ReadOther =
    //          new BufferedWriter(
    //              new OutputStreamWriter(
    //                  new FileOutputStream(
    //                      Config.CERTIFICATE_OUTPUT_FILE
    //                          + "ReadOther"
    //                          + ErrorStatistics.errorStatistics
    //                              .get(ErrorStatistics.ErrorType.READ_OTHER_ERROR)
    //                              .size()
    //                          + "-temp.json")));
    //    } catch (FileNotFoundException e) {
    //      logger.warn(e);
    //    }
    //
    //    try {
    //      operationTrace = new OperationTrace(operationTrace);
    //      if(operationTrace.getReadTupleList() != null){
    //        operationTrace.setReadTupleList(null);
    //      }
    //      tempHandler4ReadOther.write(gson.toJson(operationTrace));
    //      tempHandler4ReadOther.newLine();
    //      tempHandler4ReadOther.write(gson.toJson(tupleTrace));
    //      tempHandler4ReadOther.newLine();
    //
    // tempHandler4ReadOther.write(gson.toJson(HistoryVersion.getVersionChain(tupleTrace.getKey())));
    //      tempHandler4ReadOther.flush();
    //      tempHandler4ReadOther.close();
    //    } catch (IOException e) {
    //      logger.warn(e);
    //    }
  }

  public static void outputSerializeAccessError(
      OperationTrace operationTrace,
      Profile profile,
      Version nextVersion,
      VersionChain versionChain) {
    ErrorStatistics.increase(
        ErrorStatistics.ErrorType.SERIALIZE_ACCESS_ERROR, operationTrace.getTransactionID());
    ReplayController.addErrorTrace(operationTrace);

    ArrayList<OperationTraceJson> certificateSerializeAccess = new ArrayList<>();

    try {
      certificateHandler4SerializeAccess =
          new BufferedWriter(
              new OutputStreamWriter(
                  new FileOutputStream(
                      Config.CERTIFICATE_OUTPUT_FILE
                          + "SerializeAccess"
                          + ErrorStatistics.errorStatistics
                              .get(ErrorStatistics.ErrorType.SERIALIZE_ACCESS_ERROR)
                              .size()
                          + ".json")));
    } catch (FileNotFoundException e) {
      logger.warn(e);
    }

    certificateSerializeAccess.add(new OperationTraceJson("first-updater-wins error", null));

    OperationTrace readConsistentOperationTrace =
        new OperationTrace(
            operationTrace.getThreadID(), operationTrace.getTransactionID().split(",")[1], "0");
    readConsistentOperationTrace.setStartTime(
        profile.getConsistentReadTimeInterval().getStartTimestamp());
    readConsistentOperationTrace.setFinishTime(
        profile.getConsistentReadTimeInterval().getFinishTimestamp());
    readConsistentOperationTrace.setOperationTraceType(OperationTraceType.START);
    certificateSerializeAccess.add(
        new OperationTraceJson(
            "the operation(T1,Op1) of start consistency read: ", readConsistentOperationTrace));

    certificateSerializeAccess.add(
        new OperationTraceJson("the write operation(T1,Op2) of  T1: ", operationTrace));

    certificateSerializeAccess.add(
        new OperationTraceJson(
            "the record(R1) produced by T1,Op2: ", operationTrace.getWriteTupleList().get(0)));

    certificateSerializeAccess.add(new OperationTraceJson("the profile of T1: ", profile));

    OperationTrace firstUpdaterOperationTrace =
        new OperationTrace(
            nextVersion.getTransactionID().split(",")[0],
            nextVersion.getTransactionID().split(",")[1],
            "+inf");
    firstUpdaterOperationTrace.setStartTime(nextVersion.getStartTimestamp());
    firstUpdaterOperationTrace.setFinishTime(nextVersion.getFinishTimestamp());
    firstUpdaterOperationTrace.setOperationTraceType(OperationTraceType.COMMIT);
    certificateSerializeAccess.add(
        new OperationTraceJson(
            "the parallel write operation(T2,Op1): ", firstUpdaterOperationTrace));

    certificateSerializeAccess.add(
        new OperationTraceJson("the record(R2) produced by T2,Op1: ", nextVersion));

    certificateSerializeAccess.add(
        new OperationTraceJson("the version chain(VC) on R1: ", versionChain));

    try {
      certificateHandler4SerializeAccess.write(gson.toJson(certificateSerializeAccess));
      certificateHandler4SerializeAccess.flush();
      certificateHandler4SerializeAccess.close();
    } catch (IOException e) {
      logger.warn(e);
    }
  }

  public static void outputDependencyCycleError(List<List<Profile>> cycles) {
    ErrorStatistics.increase(
        ErrorStatistics.ErrorType.DEPENDENCY_CYCLE_ERROR, cycles.get(0).get(0).getTransactionID());

    ArrayList<OperationTraceJson> certificateDependencyCycle = new ArrayList<>();

    try {
      certificateHandler4DependencyCycle =
          new BufferedWriter(
              new OutputStreamWriter(
                  new FileOutputStream(
                      Config.CERTIFICATE_OUTPUT_FILE
                          + "DependencyCycle"
                          + ErrorStatistics.errorStatistics
                              .get(ErrorStatistics.ErrorType.DEPENDENCY_CYCLE_ERROR)
                              .size()
                          + ".json")));
    } catch (FileNotFoundException e) {
      logger.warn(e);
    }

    certificateDependencyCycle.add(new OperationTraceJson("Non-Serializability", null));

    certificateDependencyCycle.add(new OperationTraceJson("All Cycles", cycles));

    SimpleDirectedGraph<Profile, Dependency> dependencyGraph =
        new SimpleDirectedGraph<>(Dependency.class);
    int cycleSize = cycles.get(0).size();
    for (int i = 0; i < cycleSize; i++) {
      Profile profile1 = cycles.get(0).get(i);
      Profile profile2 = cycles.get(0).get((i + 1) % cycleSize);
      Dependency dependency = DependencyGraph.getGraph().getEdge(profile1, profile2);

      certificateDependencyCycle.add(new OperationTraceJson("Dependency " + i, dependency));

      certificateDependencyCycle.add(
          new OperationTraceJson(
              "Version Chain on " + dependency.getDependecnyKey(),
              HistoryVersion.getVersionChain(dependency.getDependecnyKey())));

      dependencyGraph.addVertex(profile1);
      dependencyGraph.addVertex(profile2);
      dependencyGraph.addEdge(profile1, profile2, dependency);
    }
    certificateDependencyCycle.add(
        new OperationTraceJson("First Cycle", DependencyGraph.outputGraph(dependencyGraph)));

    certificateDependencyCycle.add(
        new OperationTraceJson(
            "The Whole Dependency Graph", DependencyGraph.outputGraph(DependencyGraph.getGraph())));

    try {
      certificateHandler4DependencyCycle.write(gson.toJson(certificateDependencyCycle));
      certificateHandler4DependencyCycle.flush();
      certificateHandler4DependencyCycle.close();
    } catch (IOException e) {
      logger.warn(e);
    }
  }
}
