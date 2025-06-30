package adapter;

import java.io.Serializable;

public abstract class SQLFilter implements Serializable {
  public abstract String filter(String sql);
}
