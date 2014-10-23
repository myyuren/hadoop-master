package org.flycloud.hadoop.impala.newquery;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.flycloud.hadoop.impala.database.DBPool;
import org.flycloud.hadoop.impala.database.IResultProcessor;
import org.flycloud.hadoop.impala.thread.RunningThread;

public class QueryThread extends RunningThread {
	private String sql;

	public String getSql() {
		return sql;
	}

	public void setSql(String sql) {
		this.sql = sql;
	}

	private IResultProcessor processor;

	public IResultProcessor getProcessor() {
		return processor;
	}

	public void setProcessor(IResultProcessor processor) {
		this.processor = processor;
	}

	public Connection getFromConnectionPool() {
		return DBPool.getInstance().getConnection();
	}

	public void query(String sql) throws SQLException {
		Connection con = getFromConnectionPool();
		Statement stmt = con.createStatement();
		ResultSet rs = stmt.executeQuery(sql);
		this.processor.process(rs);
		con.close();
	}

	@Override
	public void runFunction() {
		try {
			query(sql);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

}