package com.renren.infra.xzl;

import java.util.Date;

import org.apache.log4j.PropertyConfigurator;

public class MyRunnable implements Runnable {

	public final static int THRAD_NUM = 200;

	public String connectString = "127.0.0.1:2181";
	public int sessionTimeout = 2000;
	public String prefix = "/_lock_node_";
	public String lockName = "_test_lock_";
	public int id;
	private TestLock lock;

	public MyRunnable(int id) {
		this.id = id;
		lock = new TestLock(connectString, sessionTimeout, prefix, lockName);
		lock.init();
		lock.setRunnable(new TestRunnable());
	}

	public void run() {
		// System.err.println("Thread " + id + " start.");
		lock.run();
	}

	public static void main(String[] args) throws InterruptedException {
		PropertyConfigurator.configure("conf/log4j.properties");

		Thread threads[] = new Thread[THRAD_NUM];
		for (int i = 0; i < THRAD_NUM; i++) {
			threads[i] = new Thread(new MyRunnable(i));
		}
		long start = System.currentTimeMillis();
		for (int i = 0; i < THRAD_NUM; i++) {
			threads[i].start();
		}
		for (int i = 0; i < THRAD_NUM; i++) {
			threads[i].join();
		}
		long end = System.currentTimeMillis();
		System.err.println((end - start) / 1000.0);
	}
}
