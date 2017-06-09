package com.github.blovemaple.backupd.machine;

import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.github.blovemaple.backupd.plan.BackupConf;
import com.github.blovemaple.backupd.plan.BackupTask;

/**
 * 执行备份的控制任务，负责从{@link BackupDelayingQueue}中提取{@link BackupTask}并执行。
 * 
 * @author blovemaple <blovemaple2010(at)gmail.com>
 */
public class BackupController implements Runnable {
	private static final Logger logger = LogManager.getLogger(BackupController.class);

	private final BackupDelayingQueue queue;
	private final Map<BackupConf, BackupMonitor> monitors;

	public BackupController(BackupDelayingQueue queue, Map<BackupConf, BackupMonitor> monitors) {
		this.queue = queue;
		this.monitors = monitors;
	}

	@Override
	public void run() {
		logger.info(() -> "Started backup controller.");

		ExecutorService executor = Executors.newSingleThreadExecutor();
		try {
			while (true) {
				BackupTask task = queue.fetch(10);
				if (task != null) {
					Future<Boolean> future = executor.submit(task);

					BackupMonitor monitor = monitors.get(task.conf());
					if (monitor != null)
						monitor.taskStarted(task, future);

					try {
						Boolean backuped = future.get();
						if (backuped)
							logger.info(() -> "Completed backup task " + task);
						else
							logger.info(() -> "Dropped backup task " + task);
					} catch (ExecutionException e) {
						// 为了保证不中止，只打印而不抛出异常
						logger.error(() -> "Error running backup task: " + task, e);
					} catch (CancellationException e) {
					}

				}
			}
		} catch (InterruptedException e) {
			// 线程被中断，直接结束
		} catch (Exception e) {
			logger.fatal("Backup controller exception.", e);
		} finally {
			executor.shutdownNow();
		}
	}

}
