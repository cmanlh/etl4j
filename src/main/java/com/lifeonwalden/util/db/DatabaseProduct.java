package com.lifeonwalden.util.db;

import java.util.HashMap;
import java.util.Map;

public enum DatabaseProduct {
	HSQL("HSQL Database Engine");

	private String name;

	DatabaseProduct(String name) {
		this.name = name;
	}

	public String getName() {
		return name;
	}

	private static Map<String, DatabaseProduct> mapping = new HashMap<String, DatabaseProduct>();
	static {
		for (DatabaseProduct prd : DatabaseProduct.values()) {
			mapping.put(prd.name, prd);
		}
	}

	public static DatabaseProduct forName(String name) {
		return mapping.get(name);
	}
}
