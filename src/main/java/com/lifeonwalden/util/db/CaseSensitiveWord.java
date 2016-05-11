package com.lifeonwalden.util.db;

import org.apache.commons.lang3.StringUtils;

public class CaseSensitiveWord {
	public static String toRightOne(String target, String dbPrdName) {
		switch (DatabaseProduct.forName(dbPrdName)) {
		case HSQL:
			return StringUtils.join("\"", target, "\"");
		default:
			return target;
		}
	}
}
