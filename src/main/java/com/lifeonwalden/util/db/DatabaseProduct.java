package com.lifeonwalden.util.db;

import java.util.HashMap;
import java.util.Map;

public enum DatabaseProduct {
  HSQL("HSQL Database Engine", "HSQL Database Engine Driver"), MSSQL_JTDS("Microsoft SQL Server",
      "jTDS Type 4 JDBC Driver for MS SQL Server and Sybase");

  private String prdName;

  private String driverName;

  DatabaseProduct(String prdName, String driverName) {
    this.prdName = prdName;
    this.driverName = driverName;
  }

  public String getName() {
    return prdName;
  }

  public String getDriverName() {
    return driverName;
  }

  private static Map<String, DatabaseProduct> mapping = new HashMap<String, DatabaseProduct>();
  static {
    for (DatabaseProduct prd : DatabaseProduct.values()) {
      mapping.put(prd.prdName + prd.driverName, prd);
    }
  }

  public static DatabaseProduct forPrdnDriver(String prdName, String driverName) {
    return mapping.get(prdName + driverName);
  }
}
