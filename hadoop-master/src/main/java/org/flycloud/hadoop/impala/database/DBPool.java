package org.flycloud.hadoop.impala.database;

import java.sql.Connection;
import java.sql.SQLException;
import java.beans.PropertyVetoException;

import com.mchange.v2.c3p0.ComboPooledDataSource;

public class DBPool {
	private ComboPooledDataSource dataSource;
	private static DBPool[] dbPools;

	public DBPool(String driverClass, String jdbcUrl) {
		try {
			dataSource = new ComboPooledDataSource();
			dataSource.setUser("cloudera");
			dataSource.setPassword("cloudera");
			dataSource.setJdbcUrl(jdbcUrl);
			dataSource.setDriverClass(driverClass);
			dataSource.setInitialPoolSize(16);
			dataSource.setMinPoolSize(16);
			dataSource.setMaxPoolSize(16);
			dataSource.setMaxStatements(500);
			dataSource.setMaxIdleTime(60);
		} catch (PropertyVetoException e) {
			throw new RuntimeException(e);
		}
	}

	public final synchronized static DBPool getInstance() {
		if (dbPools == null) {
			dbPools = new DBPool[3];
			dbPools[0] = new DBPool("org.apache.hive.jdbc.HiveDriver",
					"jdbc:hive2://192.168.0.2:21050/;auth=noSasl");
			dbPools[1] = new DBPool("org.apache.hive.jdbc.HiveDriver",
					"jdbc:hive2://192.168.0.3:21050/;auth=noSasl");
			dbPools[2] = new DBPool("org.apache.hive.jdbc.HiveDriver",
					"jdbc:hive2://192.168.0.4:21050/;auth=noSasl");
		}
		int idx = (int) (Math.random() * 3);
		return dbPools[idx];
		
//		if (dbPools == null) {
//			dbPools = new DBPool[1];
//			dbPools[0] = new DBPool("org.apache.hive.jdbc.HiveDriver",
//					"jdbc:hive2://192.168.0.1:21050/;auth=noSasl");
//		}
//		return dbPools[0];
	}

	public final Connection getConnection() {
		try {
			return dataSource.getConnection();
		} catch (SQLException e) {
			throw new RuntimeException("无法从数据源获取连接 ", e);
		}
	}

	public static void main(String[] args) throws SQLException {
		Connection con = null;
		try {
			con = DBPool.getInstance().getConnection();
		} catch (Exception e) {
		} finally {
			if (con != null)
				con.close();
		}
	}
}