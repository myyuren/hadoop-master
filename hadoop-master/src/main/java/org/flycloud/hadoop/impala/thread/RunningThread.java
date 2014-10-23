package org.flycloud.hadoop.impala.thread;

import java.util.Date;


public abstract class RunningThread implements Runnable {
	private int index;
	private Director director;
	
	public void run() {
		this.director.run(index);
		long begin = new Date().getTime();
		this.runFunction();
		long end = new Date().getTime();
		long time = end - begin;
		System.out.println("耗時：" + time);
		this.director.ovr(index, time);
	}

	public Director getDirector() {
		return director;
	}

	public void setDirector(Director director) {
		this.director = director;
	}
	
	public abstract void runFunction();

	public int getIndex() {
		return index;
	}

	public void setIndex(int index) {
		this.index = index;
	}
}
