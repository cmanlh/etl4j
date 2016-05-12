package com.lifeonwalden.etl4j.workflow;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.lifeonwalden.etl4j.BaseTest;
import com.lifeonwalden.etl4j.metadata.Column;

public class ExtractTest extends BaseTest {
  @Test
  public void execute_table() throws SQLException, ClassNotFoundException {
    Class.forName("org.hsqldb.jdbcDriver");
    Connection connection = DriverManager.getConnection(CONNECTION_STRING, USER_NAME, PASSWORD);

    Extract extract = new Extract("User", connection);
    extract.query(null);

    Assert.assertFalse(extract.getColumnLabelMapping().isEmpty());

    connection.close();
  }

  @Test
  public void execute_sql() throws SQLException, ClassNotFoundException {
    Class.forName("org.hsqldb.jdbcDriver");
    Connection connection = DriverManager.getConnection(CONNECTION_STRING, USER_NAME, PASSWORD);

    Extract extract = new Extract("select \"id\" from \"User\"", connection);
    extract.query(null);

    ImmutableMap<String, Column> mapping = extract.getColumnLabelMapping();
    Assert.assertTrue(mapping.containsKey("id") && !mapping.containsKey("income"));

    connection.close();
  }

  @Test
  public void execute_withParam() throws SQLException, ClassNotFoundException {
    Class.forName("org.hsqldb.jdbcDriver");
    Connection connection = DriverManager.getConnection(CONNECTION_STRING, USER_NAME, PASSWORD);

    Extract extract = new Extract("select \"id\" from \"User\" where \"id\"=?", connection);
    extract.query(Arrays.asList("1111"));

    Assert.assertFalse(extract.getColumnLabelMapping().isEmpty());

    connection.close();
  }

  @Test
  public void execute_withParamWithNull() throws SQLException, ClassNotFoundException {
    Class.forName("org.hsqldb.jdbcDriver");
    Connection connection = DriverManager.getConnection(CONNECTION_STRING, USER_NAME, PASSWORD);

    Extract extract = new Extract("select \"id\" from \"User\" where \"id\"=? and \"birthday\" is null", connection);
    extract.query(Arrays.asList("3333"));
    extract.next();
    Assert.assertTrue(extract.fetchNextWithIndex().get(0).equals("3333"));

    connection.close();
  }

  @Test
  public void execute_resultLabel() throws SQLException, ClassNotFoundException {
    Class.forName("org.hsqldb.jdbcDriver");
    Connection connection = DriverManager.getConnection(CONNECTION_STRING, USER_NAME, PASSWORD);

    Extract extract = new Extract("select \"id\" from \"User\" where \"id\"=?", connection);
    extract.query(Arrays.asList("1111"));

    Assert.assertTrue(extract.next());
    Assert.assertTrue(extract.fetchNextWithLabel().get("id").equals("1111"));

    connection.close();
  }

  @Test
  public void execute_resultIndex() throws SQLException, ClassNotFoundException {
    Class.forName("org.hsqldb.jdbcDriver");
    Connection connection = DriverManager.getConnection(CONNECTION_STRING, USER_NAME, PASSWORD);

    Extract extract = new Extract("select \"id\" from \"User\" where \"id\"=?", connection);
    extract.query(Arrays.asList("1111"));

    Assert.assertTrue(extract.next());
    Assert.assertTrue(extract.fetchNextWithIndex().get(0).equals("1111"));

    connection.close();
  }

  @Test
  public void execute_reWriteParam() throws SQLException, ClassNotFoundException {
    Class.forName("org.hsqldb.jdbcDriver");
    Connection connection = DriverManager.getConnection(CONNECTION_STRING, USER_NAME, PASSWORD);

    Extract extract = new Extract("select \"id\" from \"User\" where \"id\"=?", connection);

    extract.query(Arrays.asList("1111"));
    Assert.assertTrue(extract.next());
    Assert.assertTrue(extract.fetchNextWithLabel().get("id").equals("1111"));

    extract.query(Arrays.asList("2222"));
    Assert.assertTrue(extract.next());
    Assert.assertTrue(extract.fetchNextWithLabel().get("id").equals("2222"));

    connection.close();
  }
}
