package io;

import gen.operation.TransactionCaseRepo;
import gen.schema.Schema;
import gen.shadow.MirrorData;
import lombok.Data;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode(callSuper = true)
@Data
public class ObjectCollection extends Storable {
  private final Schema schema;
  private final TransactionCaseRepo transactionCaseRepo;
  private final MirrorData mirrorData;
  private static final long serialVersionUID = 1L;

  public ObjectCollection(
      Schema schema, TransactionCaseRepo transactionCaseRepo, MirrorData mirrorData) {
    super();
    this.schema = schema;
    this.transactionCaseRepo = transactionCaseRepo;
    this.mirrorData = mirrorData;
  }
}
