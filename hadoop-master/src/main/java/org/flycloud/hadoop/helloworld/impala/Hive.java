package org.flycloud.hadoop.helloworld.impala;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class Hive {
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
				"jdbc:hive2://localhost:21050/;auth=noSasl", "cloudera", "cloudera");
		Statement stmt = con.createStatement();

		//
		sql = "create database if not exists snx";
		stmt.execute(sql);
		
		//
		sql = "drop table if exists user";
		stmt.execute(sql);
		
		// create table
		sql = "create table if not exists user(id int, age double, name string)"
				+ " row format delimited fields terminated by ','"
				+ " lines terminated by '\n'"
				+ " stored as textfile";
		stmt.execute(sql);
		sql = "load data inpath '/user/cloudera/tmp' overwrite into table user";
		System.out.println("Running: " + sql);
		res = stmt.executeQuery(sql);
		while (res.next()) {
			System.out.println(res.getString(1));
		}
		
		//
		sql = "show tables ";
		System.out.println("Running: " + sql);
		res = stmt.executeQuery(sql);
		while (res.next()) {
			System.out.println(res.getString(1));
		}

		sql = "describe user";
		System.out.println("Running: " + sql);
		res = stmt.executeQuery(sql);
		while (res.next()) {
			System.out.println(res.getString(1) + "\t" + res.getString(2));
		}
		
		sql = "select id,age,name from user order by id limit 10";
		res = stmt.executeQuery(sql);
		int i = 0;
		while (res.next()) {
			System.out.println(i++ + "\t" + res.getInt(1) + "\t"
					+ res.getDouble(2) + "\t" + res.getString(3));
		}
	}
}