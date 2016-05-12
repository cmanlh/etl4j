package com.lifeonwalden.etl4j.workflow;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.lifeonwalden.etl4j.BaseTest;

public class LoadTest extends BaseTest {
  @Before
  public void setup() throws ClassNotFoundException, SQLException {
    super.setup();

    Class.forName("org.hsqldb.jdbcDriver");

    Connection connection = DriverManager.getConnection(CONNECTION_STRING, USER_NAME, PASSWORD);
    connection.createStatement().executeUpdate(
        "create table \"People\" (\"id\" varchar(32), \"age\" int, \"income\" decimal(20,6), \"birthday\" datetime)");
  }

  @Test
  public void paramMetadata_insert() throws SQLException, ClassNotFoundException {
    Class.forName("org.hsqldb.jdbcDriver");

    Connection connection = DriverManager.getConnection(CONNECTION_STRING, USER_NAME, PASSWORD);

    Load load = new Load("insert into \"People\" values(?,?,?,?)", connection);

    Assert.assertFalse(load.getParamMapping().isEmpty());

    connection.close();
  }

  @Test
  public void paramMetadata_update() throws SQLException, ClassNotFoundException {
    Class.forName("org.hsqldb.jdbcDriver");

    Connection connection = DriverManager.getConnection(CONNECTION_STRING, USER_NAME, PASSWORD);

    Load load = new Load("update \"People\" set \"age\" = ? where \"id\"=?", connection);

    Assert.assertFalse(load.getParamMapping().isEmpty());

    connection.close();
  }

  @Test
  public void paramMetadata_part() throws SQLException, ClassNotFoundException {
    Class.forName("org.hsqldb.jdbcDriver");

    Connection connection = DriverManager.getConnection(CONNECTION_STRING, USER_NAME, PASSWORD);

    Load load = new Load("insert into \"People\"(\"id\",\"age\") values(?,?)", connection);

    Assert.assertFalse(load.getParamMapping().isEmpty());

    connection.close();
  }

  @Test
  public void load_full() throws SQLException, ClassNotFoundException {
    Class.forName("org.hsqldb.jdbcDriver");

    Connection connection = DriverManager.getConnection(CONNECTION_STRING, USER_NAME, PASSWORD);

    Load load = new Load("insert into \"People\" values(?,?,?,?)", connection);
    Extract extract = new Extract("User", connection);
    extract.query(null);
    while (extract.next()) {
      load.writeAtIndex(extract.fetchNextWithIndex());
    }
    load.close();
    extract.close();

    Extract extractPeople = new Extract("People", connection);
    extractPeople.query(null);
    Assert.assertTrue(extractPeople.next());
  }

  @Test
  public void load_part() throws SQLException, ClassNotFoundException {
    Class.forName("org.hsqldb.jdbcDriver");

    Connection connection = DriverManager.getConnection(CONNECTION_STRING, USER_NAME, PASSWORD);

    Load load = new Load("insert into \"People\"(\"id\",\"birthday\") values(?,?)", connection);
    Extract extract = new Extract("select \"id\",\"birthday\" from \"User\"", connection);
    extract.query(null);
    while (extract.next()) {
      load.writeAtIndex(extract.fetchNextWithIndex());
    }
    load.close();
    extract.close();

    Extract extractPeople = new Extract("People", connection);
    extractPeople.query(null);
    Assert.assertTrue(extractPeople.next());
  }

  @Test
  public void load_mixOrder() throws SQLException, ClassNotFoundException {
    Class.forName("org.hsqldb.jdbcDriver");

    Connection connection = DriverManager.getConnection(CONNECTION_STRING, USER_NAME, PASSWORD);

    Load load = new Load("insert into \"People\"(\"id\",\"birthday\",\"income\", \"age\") values(?,?,?,?)", connection);
    Extract extract = new Extract("select \"id\",\"birthday\",\"income\", \"age\" from \"User\"", connection);
    extract.query(null);
    while (extract.next()) {
      load.writeAtIndex(extract.fetchNextWithIndex());
    }
    load.close();
    extract.close();

    Extract extractPeople = new Extract("People", connection);
    extractPeople.query(null);
    Assert.assertTrue(extractPeople.next());
  }

  @Test
  public void load_null() throws SQLException, ClassNotFoundException {
    Class.forName("org.hsqldb.jdbcDriver");

    Connection connection = DriverManager.getConnection(CONNECTION_STRING, USER_NAME, PASSWORD);

    Load load = new Load("insert into \"People\"(\"id\",\"birthday\",\"income\", \"age\") values(?,?,?,?)", connection);
    Extract extract = new Extract("select \"id\",null,\"income\", \"age\" from \"User\"", connection);
    extract.query(null);
    while (extract.next()) {
      load.writeAtIndex(extract.fetchNextWithIndex());
    }
    load.close();
    extract.close();

    Extract extractPeople = new Extract("People", connection);
    extractPeople.query(null);
    Assert.assertTrue(extractPeople.next());
  }
}
