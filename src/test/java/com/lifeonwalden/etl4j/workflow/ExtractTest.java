package com.lifeonwalden.etl4j.workflow;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.junit.Test;

import com.lifeonwalden.etl4j.BaseTest;

public class ExtractTest extends BaseTest {
  @Test
	public void execute_table() throws SQLException, ClassNotFoundException {
		Class.forName("org.hsqldb.jdbcDriver");
		Connection connection = DriverManager.getConnection(CONNECTION_STRING, USER_NAME, PASSWORD);

		new Extract("User", connection)
	}
}
