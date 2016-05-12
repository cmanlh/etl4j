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
            Object param = paramList.get(i);
            if (param instanceof BigDecimal) {
                statement.setBigDecimal(i + 1, (BigDecimal) param);
            } else if (param instanceof Boolean) {
                statement.setBoolean(i + 1, (Boolean) param);
            } else if (param instanceof Byte) {
                statement.setByte(i + 1, (Byte) param);
            } else if (param instanceof Date) {
                statement.setDate(i + 1, (Date) param);
            } else if (param instanceof Double) {
                statement.setDouble(i + 1, (Double) param);
            } else if (param instanceof Float) {
                statement.setFloat(i + 1, (Float) param);
            } else if (param instanceof Integer) {
                statement.setInt(i + 1, (Integer) param);
            } else if (param instanceof Long) {
                statement.setLong(i + 1, (Long) param);
            } else if (param instanceof Short) {
                statement.setShort(i + 1, (Short) param);
            } else if (param instanceof Time) {
                statement.setTime(i + 1, (Time) param);
            } else if (param instanceof Timestamp) {
                statement.setTimestamp(i + 1, (Timestamp) param);
            } else {
                statement.setObject(i + 1, param);
            }
        }
    }
}
