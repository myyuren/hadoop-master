package org.flycloud.hadoop.data;

import java.util.LinkedHashMap;
import java.util.Set;

import org.flycloud.hadoop.data.fields.Field;

public class Table {
	private int count;
	private LinkedHashMap<String, Field> fields = new LinkedHashMap<String, Field>();

	public Table(int count) {
		this.setCount(count);
	}

	public Field createField(String name, Field field) {
		return fields.put(name, field);
	}
	
	public Field getField(String name) {
		return fields.get(name);
	}
	
	public void setField(String name, Field field) {
		this.fields.put(name, field);
	}
	
	public Set<String> getFieldsName() {
		return this.fields.keySet();
	}

	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}
	
}
