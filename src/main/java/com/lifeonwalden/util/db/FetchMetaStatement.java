package com.lifeonwalden.util.db;

import org.apache.commons.lang3.StringUtils;

public class FetchMetaStatement {
    public static String fetch(String target, String prdName, String driverName) {
        switch (DatabaseProduct.forPrdnDriver(prdName, driverName)) {
            case HSQL:
            case MSSQL_JTDS:
                return StringUtils.join("select top 1 * from ", CaseSensitiveWord.toRightOne(target, prdName, driverName));
            default:
                return "select * from " + target;
        }
    }
}
