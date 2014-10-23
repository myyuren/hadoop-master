package org.flycloud.hadoop.impala.thread;

import java.util.Date;

public class Director {
	private int maxCount = 100;// 最大线程数量
	private int curCount = 0;// 当前的线程数量
	private int totCount = 0; // 总共启动了多少
	private int ovrCount = 0; // 总共执行完多少
	private long totTime = 0; //
	private long maxTime = Long.MIN_VALUE; //
	private long minTime = Long.MAX_VALUE; //
	private ThreadFactory threadFactory;
	private boolean stoped = false;
	private long beginTime = 0;

	public synchronized void run(int index) {
		curCount++;
	}

	public synchronized void ovr(int index, long time) {
		curCount--;
		ovrCount++;
		totTime += time;
		if (time > maxTime) {
			maxTime = time;
		}
		if (time < minTime) {
			minTime = time;
		}
		if (!stoped) {
			int create = maxCount - curCount;
			this.create(create);
		}
	}

	public synchronized void stp() {
		stoped = true;
		if (ovrCount == 0) {
			System.out.println("完成0条。");
			return;
		}
		long avr = totTime / ovrCount;//平均耗时
		long tot = new Date().getTime() - this.beginTime;
		System.out.println("执行总时间：" + tot);
		System.out.println("执行并发数：" + this.getMaxCount());
		System.out.println("启动总条数：" + totCount);
		System.out.println("执行完条数：" + ovrCount);
		System.out.println("平均条用时：" + avr);
		System.out.println("最大条用时：" + maxTime);
		System.out.println("最小条用时：" + minTime);
	}

	public void begin() {
		curCount = 0;
		stoped = false;
		totCount = 0;
		this.beginTime = new Date().getTime();
		this.create(maxCount);
	}

	private void create(int count) {
		for (int i = 0; i < count; i++) {
			threadFactory.create(totCount++);
		}
	}

	public int getMaxCount() {
		return maxCount;
	}

	public void setMaxCount(int maxCount) {
		this.maxCount = maxCount;
	}

	public int getCurCount() {
		return curCount;
	}

	public void setCurCount(int curCount) {
		this.curCount = curCount;
	}

	public ThreadFactory getThreadFactory() {
		return threadFactory;
	}

	public void setThreadFactory(ThreadFactory threadFactory) {
		this.threadFactory = threadFactory;
	}

	public int getTotCount() {
		return totCount;
	}

	public void setTotCount(int totCount) {
		this.totCount = totCount;
	}
}
