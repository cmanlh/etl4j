package com.lifeonwalden.etl4j.workflow;

import java.sql.Connection;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import com.lifeonwalden.etl4j.metadata.Param;
import com.lifeonwalden.util.db.PreparedStatementHanlder;

public class Load {
  private Connection connection;
  private String sql;
  private long counter;
  private long buffCounter;
  private PreparedStatement statement;
  private int blockSize = 5000;
  private ImmutableMap<Integer, Param> paramMapping;

  public Load(String sql, Connection connection) throws SQLException {
    this.sql = sql;
    this.connection = connection;

    init();
  }

  private void init() throws SQLException {
    statement = connection.prepareStatement(sql);
    ParameterMetaData pmd = statement.getParameterMetaData();
    Map<Integer, Param> _paramMapping = new HashMap<Integer, Param>();
    int paramCount = pmd.getParameterCount();
    try {
      for (int i = 1; i <= paramCount; i++) {
        Param param =
            new Param().setClazz(Class.forName(pmd.getParameterClassName(i))).setIndex(i)
                .setType(pmd.getParameterType(i)).setMode(pmd.getParameterMode(i));
        _paramMapping.put(i, param);
      }
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
    paramMapping = ImmutableMap.copyOf(_paramMapping);
  }

  public ImmutableMap<Integer, Param> getParamMapping() {
    return paramMapping;
  }

  public long getCounter() {
    return counter;
  }

  public int getBlockSize() {
    return blockSize;
  }

  public void setBlockSize(int blockSize) {
    this.blockSize = blockSize;
  }

  public void writeAtLabel(Map<String, Object> paramPackage, InOutParamHandler paramHandler) throws SQLException {
    paramHandler.setParamAtLabel(statement, paramPackage);
    write();
  }

  public void writeAtIndex(List<Object> paramList) throws SQLException {
    PreparedStatementHanlder.setParam(statement, paramMapping, paramList);
    write();
  }

  private void write() throws SQLException {
    statement.addBatch();
    buffCounter++;
    if (buffCounter >= blockSize) {
      execute();
    }
  }

  private void execute() throws SQLException {
    for (int cnt : statement.executeBatch()) {
      counter += cnt;
    }
    connection.commit();
    statement.clearBatch();
    statement.clearParameters();
    buffCounter = 0;
  }

  public void commit() throws SQLException {
    execute();

    connection.commit();
  }

  public void close() throws SQLException {
    try {
      commit();
    } finally {
      statement.close();
    }
  }
}
