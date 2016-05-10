package com.lifeonwalden.etl4j.metadata.transfer;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.commons.lang3.StringUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


public class MetaDataToDBBeanTransferTest {
    private static final String CONNECTION_STRING = "jdbc:hsqldb:mem:testdb;shutdown=false";
    private static final String USER_NAME = "SA";
    private static final String PASSWORD = StringUtils.EMPTY;

    @Before
    public void setup() throws ClassNotFoundException, SQLException {
        Class.forName("org.hsqldb.jdbcDriver");

        Connection connection = DriverManager.getConnection(CONNECTION_STRING, USER_NAME, PASSWORD);
        connection.createStatement().executeUpdate(
                "create table \"User\" (\"id\" varchar(32), \"age\" int, \"income\" decimal(20,6), \"birthday\" datetime)");
        PreparedStatement ps = connection.prepareStatement("insert into \"User\" values(?,?,?,?)");
        ps.setString(1, "1111");
        ps.setInt(2, 50);
        ps.setDouble(3, 5.6);
        ps.setDate(4, new Date(System.currentTimeMillis()));
        ps.addBatch();
        ps.setString(1, "2222");
        ps.setInt(2, 35);
        ps.setDouble(3, 8.6);
        ps.setDate(4, new Date(System.currentTimeMillis()));
        ps.addBatch();
        ps.executeBatch();
        connection.close();
    }

    @After
    public void clear() throws ClassNotFoundException, SQLException {
        Class.forName("org.hsqldb.jdbcDriver");

        Connection connection = DriverManager.getConnection(CONNECTION_STRING, USER_NAME, PASSWORD);
        Statement stmt = connection.createStatement();
        stmt.execute("DROP SCHEMA PUBLIC CASCADE");
        connection.close();
    }

    @Test
    public void transfer_emptyPackage() throws SQLException, ClassNotFoundException {
        Class.forName("org.hsqldb.jdbcDriver");
        Connection connection = DriverManager.getConnection(CONNECTION_STRING, USER_NAME, PASSWORD);

        Assert.assertFalse(StringUtils.isNotBlank(new MetaDataToDBBeanTransfer("", null).transfer(connection.createStatement()
                .executeQuery("select * from \"User\"").getMetaData())));

        connection.close();
    }

    @Test
    public void transfer_nullPackage() throws SQLException, ClassNotFoundException {
        Class.forName("org.hsqldb.jdbcDriver");
        Connection connection = DriverManager.getConnection(CONNECTION_STRING, USER_NAME, PASSWORD);


        try {
            StringUtils.isNotBlank(new MetaDataToDBBeanTransfer(null, null).transfer(connection.createStatement()
                    .executeQuery("select * from \"User\"").getMetaData()));
        } catch (NullPointerException e) {
            Assert.assertTrue(true);
            return;
        } finally {
            connection.close();
        }

        Assert.assertTrue(false);
    }
}
