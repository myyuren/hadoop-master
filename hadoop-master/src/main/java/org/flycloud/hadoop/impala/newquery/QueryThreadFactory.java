package org.flycloud.hadoop.impala.newquery;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.flycloud.hadoop.impala.database.IResultProcessor;
import org.flycloud.hadoop.impala.thread.Director;
import org.flycloud.hadoop.impala.thread.RunningThread;
import org.flycloud.hadoop.impala.thread.ThreadFactory;

public class QueryThreadFactory implements ThreadFactory, IResultProcessor {
	private Director director;
	private String sql;

	@Override
	public RunningThread create(int i) {
		QueryThread qt = new QueryThread();
		qt.setDirector(director);
		qt.setSql(sql);
		qt.setProcessor(this);
		qt.setIndex(i);
		new Thread(qt).start();
		return qt;
	}
	
	public Director getDirector() {
		return director;
	}

	public void setDirector(Director director) {
		this.director = director;
	}

	public String getSql() {
		return sql;
	}

	public void setSql(String sql) {
		this.sql = sql;
	}

	@Override
	public void process(ResultSet res) throws SQLException {
		while (res.next()) {
			String x = res.getString(1) + "\t" + res.getString(2)
					+ "\t" + res.getString(3);
//			System.out.println(x);
		}
	}

}
