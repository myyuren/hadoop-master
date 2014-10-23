package org.flycloud.hadoop.impala.database;

import java.sql.ResultSet;
import java.sql.SQLException;

public interface IResultProcessor {
	public void process(ResultSet resultSet) throws SQLException;
}
