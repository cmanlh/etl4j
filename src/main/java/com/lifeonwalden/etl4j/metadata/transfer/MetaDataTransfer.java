package com.lifeonwalden.etl4j.metadata.transfer;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public interface MetaDataTransfer {
	public String transfer(ResultSetMetaData rsmd) throws SQLException;
}
