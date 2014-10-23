package org.flycloud.hadoop.data.fields;

import java.util.ArrayList;
import java.util.List;

public class EnumRandomField implements Field {
	private List<byte[]> list = new ArrayList<byte[]>();
	
	public EnumRandomField(byte[]... list) {
		for (byte[] l : list) {
			this.list.add(l);
		}
	}
	
	public EnumRandomField(String... list) {
		for (String l : list) {
			this.list.add(l.getBytes());
		}
	}

	@Override
	public synchronized byte[] random() {
		int idx = (int) (Math.random()*list.size());
		return this.list.get(idx);
	}

}
