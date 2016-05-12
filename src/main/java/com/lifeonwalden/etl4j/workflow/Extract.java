package com.lifeonwalden.etl4j.workflow;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
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
import com.lifeonwalden.etl4j.metadata.Column;
import com.lifeonwalden.util.db.CaseSensitiveWord;
import com.lifeonwalden.util.db.PreparedStatementHanlder;
import com.lifeonwalden.util.db.SqlParser;

public class Extract {
  private Connection connection;
  private String sql;
  private ResultSet rs;
  private PreparedStatement statement;
  private int fetchSize = 200;
  private ImmutableMap<String, Column> columnMapping;

  public Extract(String sql, Connection connection) {
    this.sql = sql;
    this.connection = connection;
  }

  public ImmutableMap<String, Column> getColumnMapping() {
    return columnMapping;
  }

  public void query(List<Object> paramList) throws SQLException {
    if (rs != null) {
      rs.close();
    } else {
      DatabaseMetaData dbmd = connection.getMetaData();
      statement = connection.prepareStatement(SqlParser.isValidSql(sql) ? sql
          : StringUtils.join("select * from ",
              CaseSensitiveWord.toRightOne(sql, dbmd.getDatabaseProductName(), dbmd.getDriverName())));
      statement.setFetchSize(fetchSize);
    }

    statement.clearParameters();
    PreparedStatementHanlder.setParam(statement, paramList);
    rs = statement.executeQuery();
    if (null == columnMapping) {
      Map<String, Column> _columnMapping = new HashMap<String, Column>();
      ResultSetMetaData rsmd = rs.getMetaData();
      int colCount = rsmd.getColumnCount();
      try {
        for (int i = 1; i <= colCount; i++) {
          _columnMapping.put(rsmd.getColumnLabel(i), new Column().setClazz(Class.forName(rsmd.getColumnClassName(i)))
              .setIndex(i).setName(rsmd.getColumnLabel(i)));
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

  public Map<String, Object> fetchNextAtLabel() throws SQLException {
    Map<String, Object> result = new HashMap<String, Object>();
    for (Entry<String, Column> entry : columnMapping.entrySet()) {
      Column column = entry.getValue();
      result.put(column.getName(), rs.getObject(entry.getKey(), column.getClazz()));
    }

    return result;
  }

  public Map<Integer, Object> fetchNextAtIndex() throws SQLException {
    Map<Integer, Object> result = new HashMap<Integer, Object>();
    for (Entry<String, Column> entry : columnMapping.entrySet()) {
      Column column = entry.getValue();
      result.put(column.getIndex(), rs.getObject(column.getIndex(), column.getClazz()));
    }

    return result;
  }

  public void close() throws SQLException {
    if (null != rs) {
      try {
        rs.close();
      } finally {
        if (null != statement) {
          statement.close();
        }
      }
    }
  }
}
