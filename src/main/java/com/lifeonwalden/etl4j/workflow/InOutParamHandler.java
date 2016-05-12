package com.lifeonwalden.etl4j.workflow;

import java.sql.PreparedStatement;
import java.util.Map;

public interface InOutParamHandler {
  public void setParamAtLabel(PreparedStatement statement, Map<String, Object> paramPackage);

  public void setParamAtIndex(PreparedStatement statement, Map<Integer, Object> paramPackage);
}
