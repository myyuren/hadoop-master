package org.flycloud.hadoop.impala.query;

import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ImpalaQuery {
	private static final String JDBC_HIVE = "jdbc:hive2://192.168.0.2:21050/;auth=noSasl";
	private static final String DRIVER_NAME = "org.apache.hive.jdbc.HiveDriver";

	public static List<String> loadSqlFile(FileOutputStream timefos, String res)
			throws IOException {
		BufferedReader ir = new BufferedReader(new InputStreamReader(
				ImpalaQuery.class.getResourceAsStream(res)));
		List<String> sqls = new ArrayList<String>();
		String sql = "", line;
		boolean first = true;
		while ((line = ir.readLine()) != null) {
			line = line.trim();
			if (first && line.startsWith("--")) {
				timefos.write((line + "\r\n").getBytes());
				first = false;
			}
			if (!line.startsWith("--")) {
				sql += " " + line;
				if (sql.endsWith(";")) {
					sqls.add(sql.replaceAll(";$", ""));
					sql = "";
				}
			}
		}
		if (sql.length() > 0) {
			sqls.add(sql);
		}
		ir.close();
		return sqls;
	}

	public static ResultSet executeQuery(Statement stmt, List<String> sqls)
			throws SQLException {
		int i = 0;
		ResultSet rs = null;
		for (String sql : sqls) {
			if (++i == sqls.size()) {
				rs = stmt.executeQuery(sql);
				break;
			}
			stmt.execute(sql);
		}
		return rs;
	}

	public static boolean execute(Statement stmt, List<String> sqls)
			throws SQLException {
		boolean rs = true;
		for (String sql : sqls) {
			if (!stmt.execute(sql)) {
				rs = rs && false;
			}
		}
		return rs;
	}

	public static void main(String[] args) throws SQLException, IOException {
		try {
			Class.forName(DRIVER_NAME);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			System.exit(1);
		}
		ResultSet res = null;
		Connection con = DriverManager.getConnection(JDBC_HIVE, "cloudera",
				"cloudera");
		Statement stmt = con.createStatement();

		Map<String, String> sqlls = new HashMap<String, String>();
		sqlls.put("省级全量查询明细", "/sqls/011.sql");
		sqlls.put("市级全量查询明细", "/sqls/021.sql");

		FileOutputStream timefos = new FileOutputStream("target/time.txt");
		for (String name : sqlls.keySet()) {
			List<String> sqls = loadSqlFile(timefos, sqlls.get(name));
			long f = new Date().getTime();
			res = executeQuery(stmt, sqls);
			int i = 0;
			FileOutputStream fos = new FileOutputStream("target/" + name
					+ ".out.txt");
			while (res.next()) {
				ResultSetMetaData md = res.getMetaData();
				int count = md.getColumnCount();
				int ii = 1;
				String line = "\r\n"+ i++;
				for (int jj=0; jj<count; jj++) {
					line += "\t" + res.getString(ii++);
				}
				fos.write(line.getBytes());
			}
			fos.close();
			long t = new Date().getTime();
			String x = "TIME=" + (t - f) + "ms; SQL_FILE=" + sqlls.get(name)+"\r\n";
			System.out.print(x);
			timefos.write(x.getBytes());
		}
		timefos.close();
	}
}