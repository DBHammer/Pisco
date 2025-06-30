package trace;

public enum OperationTraceType {
  SELECT,
  UPDATE,
  INSERT,
  DELETE,
  START,
  COMMIT,
  ROLLBACK,
  SET_SNAPSHOT,
  DDL,
  DistributeSchedule,
  FAULT
}
