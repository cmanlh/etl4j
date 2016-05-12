package com.lifeonwalden.util.db;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.List;

public class PreparedStatementHanlder {
  public static void setParam(PreparedStatement statement, List<Object> paramList) throws SQLException {
    if (null == paramList || paramList.isEmpty()) {
      return;
    }

    for (int i = 0; i < paramList.size(); i++) {
      setParam(statement, i + 1, paramList.get(i));
    }
  }

  public static void setParam(PreparedStatement statement, int colIndex, Object param) throws SQLException {
    if (param instanceof BigDecimal) {
      statement.setBigDecimal(colIndex, (BigDecimal) param);
    } else if (param instanceof Boolean) {
      statement.setBoolean(colIndex, (Boolean) param);
    } else if (param instanceof Byte) {
      statement.setByte(colIndex, (Byte) param);
    } else if (param instanceof Date) {
      statement.setDate(colIndex, (Date) param);
    } else if (param instanceof Double) {
      statement.setDouble(colIndex, (Double) param);
    } else if (param instanceof Float) {
      statement.setFloat(colIndex, (Float) param);
    } else if (param instanceof Integer) {
      statement.setInt(colIndex, (Integer) param);
    } else if (param instanceof Long) {
      statement.setLong(colIndex, (Long) param);
    } else if (param instanceof Short) {
      statement.setShort(colIndex, (Short) param);
    } else if (param instanceof Time) {
      statement.setTime(colIndex, (Time) param);
    } else if (param instanceof Timestamp) {
      statement.setTimestamp(colIndex, (Timestamp) param);
    } else {
      statement.setObject(colIndex, param);
    }
  }
}
