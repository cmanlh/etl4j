package com.lifeonwalden.etl4j.workflow;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang3.StringUtils;

import com.google.common.collect.ImmutableMap;
import com.lifeonwalden.etl4j.metadata.Column;
import com.lifeonwalden.etl4j.metadata.Param;
import com.lifeonwalden.util.db.CaseSensitiveWord;
import com.lifeonwalden.util.db.PreparedStatementHanlder;
import com.lifeonwalden.util.db.SqlParser;

public class Extract {
  private Connection connection;
  private String sql;
  private ResultSet rs;
  private PreparedStatement statement;
  private int fetchSize = 200;
  private ImmutableMap<String, Column> columnLabelMapping;
  private ImmutableMap<Integer, Column> columnIndexMapping;
  private ImmutableMap<Integer, Param> paramMapping;

  public Extract(String sql, Connection connection) {
    this.sql = sql;
    this.connection = connection;
  }

  public ImmutableMap<String, Column> getColumnLabelMapping() {
    return columnLabelMapping;
  }

  public ImmutableMap<Integer, Column> getColumnIndexMapping() {
    return columnIndexMapping;
  }

  public ImmutableMap<Integer, Param> getParamMapping() {
    return paramMapping;
  }

  public void query(List<Object> paramList) throws SQLException {
    if (rs != null) {
      rs.close();
    } else {
      DatabaseMetaData dbmd = connection.getMetaData();
      statement = connection.prepareStatement(SqlParser.isValidSql(sql) ? sql
          : StringUtils.join("select * from ",
              CaseSensitiveWord.toRightOne(sql, dbmd.getDatabaseProductName(), dbmd.getDriverName())));

      ParameterMetaData pmd = statement.getParameterMetaData();
      Map<Integer, Param> _paramMapping = new HashMap<Integer, Param>();
      int paramCount = pmd.getParameterCount();
      try {
        for (int i = 1; i <= paramCount; i++) {
          Param param = new Param().setClazz(Class.forName(pmd.getParameterClassName(i))).setIndex(i)
              .setType(pmd.getParameterType(i)).setMode(pmd.getParameterMode(i));
          _paramMapping.put(i, param);
        }
      } catch (ClassNotFoundException e) {
        throw new RuntimeException(e);
      }
      paramMapping = ImmutableMap.copyOf(_paramMapping);

      statement.setFetchSize(fetchSize);
    }

    statement.clearParameters();
    PreparedStatementHanlder.setParam(statement, paramMapping, paramList);
    rs = statement.executeQuery();
    if (null == columnLabelMapping) {
      Map<String, Column> _columnLabelMapping = new HashMap<String, Column>();
      Map<Integer, Column> _columnIntegerMapping = new HashMap<Integer, Column>();
      ResultSetMetaData rsmd = rs.getMetaData();
      int colCount = rsmd.getColumnCount();
      try {
        for (int i = 1; i <= colCount; i++) {
          Column column = new Column().setClazz(Class.forName(rsmd.getColumnClassName(i))).setIndex(i)
              .setName(rsmd.getColumnLabel(i));
          _columnLabelMapping.put(rsmd.getColumnLabel(i), column);
          _columnIntegerMapping.put(i, column);
        }
      } catch (ClassNotFoundException e) {
        throw new RuntimeException(e);
      }

      columnLabelMapping = ImmutableMap.copyOf(_columnLabelMapping);
    }
  }

  public boolean next() throws SQLException {
    return rs.next();
  }

  public Map<String, Object> fetchNextWithLabel() throws SQLException {
    Map<String, Object> result = new HashMap<String, Object>();
    for (Entry<String, Column> entry : columnLabelMapping.entrySet()) {
      Column column = entry.getValue();
      result.put(column.getName(), rs.getObject(column.getIndex(), column.getClazz()));
    }

    return result;
  }

  public List<Object> fetchNextWithIndex() throws SQLException {
    Object[] result = new Object[columnLabelMapping.size()];
    for (Entry<String, Column> entry : columnLabelMapping.entrySet()) {
      Column column = entry.getValue();
      result[column.getIndex() - 1] = rs.getObject(column.getIndex(), column.getClazz());
    }

    return Arrays.asList(result);
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
