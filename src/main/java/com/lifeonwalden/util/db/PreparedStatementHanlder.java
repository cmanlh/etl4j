package com.lifeonwalden.util.db;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

import com.google.common.collect.ImmutableMap;
import com.lifeonwalden.etl4j.metadata.Param;

public class PreparedStatementHanlder {
  public static void setParam(PreparedStatement statement, ImmutableMap<Integer, Param> paramMapping,
      List<Object> paramList) throws SQLException {
    if (null == paramList || paramList.isEmpty()) {
      return;
    }

    for (int i = 0; i < paramList.size(); i++) {
      int colIndex = i + 1;
      statement.setObject(colIndex, paramList.get(i), paramMapping.get(colIndex).getType());
    }
  }
}
