package com.lifeonwalden.util.db;

import org.apache.commons.lang3.StringUtils;

public class SqlParser {
  public static boolean isValidSql(String sql) {
    return StringUtils.startsWithIgnoreCase(sql, "select");
  }
}
