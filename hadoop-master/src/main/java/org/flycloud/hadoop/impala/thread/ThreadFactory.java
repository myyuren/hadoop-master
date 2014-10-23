package org.flycloud.hadoop.impala.thread;

public interface ThreadFactory {
	public RunningThread create(int i);
}
