//jiang yong
// thread pool for concurrent program
package com.easycache.sqlclient.utility;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ThreadPool {
	private static int threadNum = 100;
	private ExecutorService executor = null;
	private static ThreadPool threadPool = null;

	private ThreadPool() {
		executor = Executors.newFixedThreadPool(threadNum);
	}

	public static ThreadPool getInstance() {
		if (threadPool == null) {
			threadPool = new ThreadPool();
		}
		return threadPool;
	}

	public ExecutorService getExecutor() {
		return executor;
	}
}
