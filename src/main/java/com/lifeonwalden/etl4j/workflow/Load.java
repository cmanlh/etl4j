package com.lifeonwalden.etl4j.workflow;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import com.lifeonwalden.etl4j.metadata.Column;
import com.lifeonwalden.util.db.FetchMetaStatement;

public class Load {
  private Connection connection;
  private String table;
  private String sql;
  private ResultSet rs;
  private PreparedStatement statement;
  private int blockSize = 2000;
  private ImmutableMap<String, Column> columnMapping;

  public Load(String table, String sql, Connection connection) throws SQLException {
    this.table = table;
    this.sql = sql;
    this.connection = connection;

    init();
  }

  private void init() throws SQLException {
    DatabaseMetaData dbmd = connection.getMetaData();
    Statement _statement = connection.createStatement();
    try {
      ResultSetMetaData rsmd =
          _statement.executeQuery(FetchMetaStatement.fetch(table, dbmd.getDatabaseProductName(), dbmd.getDriverName()))
              .getMetaData();
      Map<String, Column> _columnMapping = new HashMap<String, Column>();
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
    } finally {
      _statement.close();
    }

  }

  public ImmutableMap<String, Column> getColumnMapping() {
    return columnMapping;
  }

  public void writeAtLabel(Map<String, Object> param) throws SQLException {}
}
