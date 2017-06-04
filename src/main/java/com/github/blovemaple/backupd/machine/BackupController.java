package com.github.blovemaple.backupd.machine;

import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.github.blovemaple.backupd.plan.BackupTask;

/**
 * 执行备份的控制任务，负责从{@link BackupDelayingQueue}中提取{@link BackupTask}并执行。
 * 
 * @author blovemaple <blovemaple2010(at)gmail.com>
 */
public class BackupController implements Runnable {
	private static final Logger logger = LogManager.getLogger(BackupController.class);

	private final BackupDelayingQueue queue;

	private boolean closed = false;

	public BackupController(BackupDelayingQueue queue) {
		this.queue = queue;
	}

	/**
	 * 关闭，当queue中无任务时执行结束。
	 */
	public void close() {
		closed = true;
	}

	@Override
	public void run() {
		try {
			while (true) {
				BackupTask task = queue.fetch(3);
				if (task != null) {
					try {
						task.call();
					} catch (IOException e) {
						// 为了保证任务不中止，只打印而不抛出异常
						logger.error(() -> "Error running backup task: " + task, e);
					}

				} else {
					if (closed)
						return;
				}
			}
		} catch (InterruptedException e) {
			// 线程被中断，直接结束
		}
	}

}
