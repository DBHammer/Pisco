package config.jdbc;

import lombok.Data;

@Data
public class DataSourceConfig {
  private String platform;
  private String driverClassName;
  private String url;
  private String username;
  private String password;

  public DataSourceConfig() {}
}
