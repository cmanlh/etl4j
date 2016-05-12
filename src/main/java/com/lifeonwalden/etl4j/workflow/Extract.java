package com.lifeonwalden.etl4j.workflow;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang3.StringUtils;

import com.google.common.collect.ImmutableMap;
import com.lifeonwalden.util.db.PreparedStatementHanlder;
import com.lifeonwalden.util.db.SqlParser;

public class Extract {
  private Connection connection;
  private String sql;
  private ResultSet rs;
  private PreparedStatement statement;
  private int fetchSize = 200;
  private ImmutableMap<String, Class<?>> columnMapping;

  public Extract(String sql, Connection connection) {
    this.sql = sql;
    this.connection = connection;
  }

  public ImmutableMap<String, Class<?>> getColumnMapping() {
    return columnMapping;
  }

  public void execute(List<Object> paramList) throws SQLException {
    if (rs != null) {
      rs.close();
    } else {
      statement =
          connection.prepareStatement(SqlParser.isValidSql(sql) ? sql : StringUtils.join("select * from ", sql));
      statement.setFetchSize(fetchSize);
    }

    statement.clearParameters();
    PreparedStatementHanlder.setParam(statement, paramList);
    rs = statement.executeQuery();
    if (null == columnMapping) {
      Map<String, Class<?>> _columnMapping = new HashMap<String, Class<?>>();
      ResultSetMetaData rsmd = rs.getMetaData();
      int colCount = rsmd.getColumnCount();
      try {
        for (int i = 1; i <= colCount; i++) {
          _columnMapping.put(rsmd.getColumnLabel(i), Class.forName(rsmd.getColumnClassName(i)));
        }
      } catch (ClassNotFoundException e) {
        throw new RuntimeException(e);
      }

      columnMapping = ImmutableMap.copyOf(_columnMapping);
    }
  }

  public boolean next() throws SQLException {
    return rs.next();
  }

  public Map<String, Object> fetchNext() throws SQLException {
    Map<String, Object> result = new HashMap<String, Object>();
    for (Entry<String, Class<?>> entry : columnMapping.entrySet()) {
      result.put(entry.getKey(), rs.getObject(entry.getKey(), entry.getValue()));
    }

    return result;
  }
}
