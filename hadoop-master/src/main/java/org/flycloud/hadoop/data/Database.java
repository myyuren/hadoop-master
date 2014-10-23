package org.flycloud.hadoop.data;

import java.util.LinkedHashMap;
import java.util.Set;

public class Database {
	private String name;
	private LinkedHashMap<String, Table> tables = new LinkedHashMap<String, Table>();

	public Database(String name) {
		this.name = name;
	}

	public Table getTable(String name) {
		return tables.get(name);
	}
	
	public Table createTable(String name, int count) {
		Table tb = new Table(count);
		tables.put(name, tb);
		return tb;
	}
	
	public void setTable(String name, Table table) {
		this.tables.put(name, table);
	}
	
	public Set<String> getTablesName() {
		return this.tables.keySet();
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}
	
}
