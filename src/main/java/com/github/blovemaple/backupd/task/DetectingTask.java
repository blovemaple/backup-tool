package com.github.blovemaple.backupd.task;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.github.blovemaple.backupd.machine.BackupDelayingQueue;

/**
 * 检测任务，即执行对一个{@link BackupConf}进行检测并向队列中提交{@link BackupTask}的任务。
 * 
 * @author blovemaple <blovemaple2010(at)gmail.com>
 */
public class DetectingTask implements Runnable {
	private static final Logger logger = LogManager.getLogger(DetectingTask.class);

	public static Integer READY_WAITING_SECONDS = 5;

	private final BackupConf conf;
	private final BackupDelayingQueue queue;

	@SuppressWarnings("unused")
	private boolean running = false;

	public DetectingTask(BackupConf conf, BackupDelayingQueue queue) {
		conf.validate();
		this.conf = conf;
		this.queue = queue;
	}

	@Override
	public synchronized void run() {
		logger.info(() -> "Started detecting task for " + conf);

		running = true;

		ExecutorService executor = Executors.newCachedThreadPool();
		Future<?> realTimeDetecting = null, fullDetecting;

		try {
			switch (conf.getType()) {
			case ONCE:
				// ONCE备份任务，如果可以备份就执行一次全量检测并等待完成
				if (checkBackupable(true)) {
					fullDetecting = executor.submit(new FullDetectingTask(conf, queue));
					fullDetecting.get();
				}
				break;
			case DAEMON:
				// DAEMON备份任务
				while (true) {
					// 如果backupconf有问题不可备份，则等待
					if (!checkBackupable(true)) {
						do {
							TimeUnit.SECONDS.sleep(READY_WAITING_SECONDS);
						} while (!checkBackupable(false));
					}
					realTimeDetecting = executor.submit(new RealTimeDetectingTask(conf, queue));
					fullDetecting = executor.submit(new FullDetectingTask(conf, queue));
					// 等待完整检测完毕
					fullDetecting.get();
					// hold在实时检测任务上
					realTimeDetecting.get();
					// 实时检测任务退出，说明有问题不可备份，回到循环起点重新等待
				}
			}
		} catch (InterruptedException e) {
		} catch (Exception e) {
			// plan执行被interrupt，或某任务发生异常，则强行中止所有任务
			logger.error(() -> "Unknown error in plan " + this, e);
		} finally {
			executor.shutdownNow();
			running = false;

			logger.info(() -> "Ended detecting task for " + conf);
		}
	}

	private boolean checkBackupable(boolean printReason) {
		try {
			conf.checkReady();
			return true;
		} catch (BackupConfNotReadyException e) {
			if (printReason)
				logger.error(() -> "Backup conf is not ready(" + e.getLocalizedMessage() + "): " + conf);
			return false;
		}
	}

	@Override
	public String toString() {
		return "BackupPlan [conf=" + conf + "]";
	}

}
