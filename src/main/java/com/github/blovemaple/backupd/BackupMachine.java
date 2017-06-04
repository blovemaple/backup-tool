package com.github.blovemaple.backupd;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.github.blovemaple.backupd.task.BackupControllerTask;

/**
 * @author blovemaple <blovemaple2010(at)gmail.com>
 */
public class BackupMachine implements Closeable {
	private final BackupDelayingQueue queue;
	private final BackupControllerTask backupController;
	private final Map<BackupConf, BackupPlan> backupPlans = new HashMap<>();

	private final ExecutorService executor;

	private boolean closed = false;

	public BackupMachine() {
		queue = new BackupDelayingQueue();
		backupController = new BackupControllerTask(queue);

		executor = Executors.newCachedThreadPool();
		executor.submit(backupController);
	}

	public void startPlan(BackupConf conf) throws IOException {
		if (closed)
			throw new IllegalStateException("Already closed.");

		BackupPlan plan = new BackupPlan(conf, queue);
		backupPlans.put(conf, plan);
		executor.submit(plan);
	}

	@Override
	public void close() {
		if (!closed) {
			closed = true;
			executor.shutdownNow();
			queue.close();
		}
	}
}
