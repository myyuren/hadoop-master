package org.flycloud.hadoop.data.fields;

public class FixedField implements Field {

	private byte[] data;
	
	public FixedField(byte[] data) {
		this.data = data;
	}
	
	@Override
	public byte[] random() {
		return data;
	}

}
