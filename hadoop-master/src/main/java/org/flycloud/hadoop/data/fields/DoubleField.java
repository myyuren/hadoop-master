package org.flycloud.hadoop.data.fields;


public class DoubleField implements Field {

	@Override
	public synchronized byte[] random() {
		return new Double(Math.random()).toString().getBytes();
	}

}
