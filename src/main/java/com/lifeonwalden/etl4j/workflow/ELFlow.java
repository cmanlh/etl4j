package com.lifeonwalden.etl4j.workflow;

import java.sql.Connection;

public interface ELFlow {
  public void extract(String query, Connection connection);

  public void load(String sql, Connection connection);
}
