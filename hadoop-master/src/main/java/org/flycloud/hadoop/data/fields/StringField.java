package org.flycloud.hadoop.data.fields;


public class StringField implements Field {

	@Override
	public synchronized byte[] random() {
		return new Double(Math.random()).toString().getBytes();
	}

}
