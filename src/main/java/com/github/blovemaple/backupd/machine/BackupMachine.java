package com.github.blovemaple.backupd.machine;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.github.blovemaple.backupd.plan.BackupConf;
import com.github.blovemaple.backupd.plan.DetectingTask;

/**
 * （非线程安全）
 * 
 * @author blovemaple <blovemaple2010(at)gmail.com>
 */
public class BackupMachine implements Closeable {
	private final BackupDelayingQueue queue;
	private final BackupController backupController;
	private final Map<BackupConf, BackupMonitor> monitors = Collections.synchronizedMap(new HashMap<>());

	private final ExecutorService executor;

	private boolean closed = false;

	public BackupMachine() {
		queue = new BackupDelayingQueue(monitors);
		backupController = new BackupController(queue, monitors);

		executor = Executors.newCachedThreadPool();
		executor.submit(backupController);
	}

	public BackupMonitor execute(BackupConf conf) throws IOException {
		if (closed)
			throw new IllegalStateException("Already closed.");

		BackupMonitor monitor;
		synchronized (monitors) {
			monitor = monitors.get(conf);
			if (monitor != null && !monitor.isDone())
				return monitor;

			monitor = new BackupMonitor(conf);
			monitors.put(conf, monitor);
		}

		DetectingTask detecting = new DetectingTask(conf, queue);
		Future<?> detectingFuture = executor.submit(detecting);

		monitor.detectingStarted(detectingFuture);
		return monitor;
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
