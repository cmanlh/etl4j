package com.lifeonwalden.util.db;

import org.apache.commons.lang3.StringUtils;

public class CaseSensitiveWord {
    public static String toRightOne(String target, String prdName, String driverName) {
        switch (DatabaseProduct.forPrdnDriver(prdName, driverName)) {
            case HSQL:
                return StringUtils.join("\"", target, "\"");
            case MSSQL_JTDS:
            default:
                return target;
        }
    }
}
