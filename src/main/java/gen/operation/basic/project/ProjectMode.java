package gen.operation.basic.project;

import java.io.Serializable;

public enum ProjectMode implements Serializable {
  PrimaryKeyOnly,
  NoPrimaryKey,
  AllowAll,
}
