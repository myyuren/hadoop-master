package org.flycloud.hadoop.data.fields;


public class IntField implements Field {
	private int max = 100;
	private int min = 0;
	
	public IntField(int min, int max) {
		super();
		this.min = min;
		this.max = max;
	}

	@Override
	public byte[] random() {
		int r = (int) ((max - min) * Math.random());
		return new Integer(min + r).toString().getBytes();
	}

}
