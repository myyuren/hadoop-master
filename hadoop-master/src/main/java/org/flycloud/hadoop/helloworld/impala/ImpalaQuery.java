package org.flycloud.hadoop.helloworld.impala;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class ImpalaQuery {
	private static String driverName = "org.apache.hive.jdbc.HiveDriver";

	/**
	 * @param args
	 * @throws SQLException
	 */
	public static void main(String[] args) throws SQLException {
		try {
			Class.forName(driverName);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			System.exit(1);
		}
		ResultSet res = null;
		String sql = null;
		Connection con = DriverManager.getConnection(
				"jdbc:hive2://192.168.0.199:21050/;auth=noSasl", "cloudera", "cloudera");
		Statement stmt = con.createStatement();
		
		sql = "select id,age,name from user order by id limit 10";
		res = stmt.executeQuery(sql);
		int i = 0;
		while (res.next()) {
			System.out.println(i++ + "\t" + res.getInt(1) + "\t"
					+ res.getDouble(2) + "\t" + res.getString(3));
		}
	}
}