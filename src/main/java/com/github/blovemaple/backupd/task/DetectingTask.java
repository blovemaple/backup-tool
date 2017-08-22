package com.github.blovemaple.backupd.task;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.github.blovemaple.backupd.machine.BackupDelayingQueue;
import com.github.blovemaple.backupd.task.BackupConf.BackupConfType;

/**
 * 检测任务，即执行对一个{@link BackupConf}进行检测并向队列中提交{@link BackupTask}的任务。
 * 
 * @author blovemaple <blovemaple2010(at)gmail.com>
 */
public class DetectingTask implements Runnable {
	private static final Logger logger = LogManager.getLogger(DetectingTask.class);

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
			// 如果conf是daemon的，则先开启实时检测
			if (conf.getType() == BackupConfType.DAEMON) {
				realTimeDetecting = executor.submit(new RealTimeDetectingTask(conf, queue));
			}

			// 无论daemon还是一次性备份，都要执行完整检测
			fullDetecting = executor.submit(new FullDetectingTask(conf, queue));

			// 等待完整检测完毕
			fullDetecting.get();
			if (realTimeDetecting != null)
				// hold在实时检测任务上（不会结束，只能等interrupt）
				realTimeDetecting.get();
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

	@Override
	public String toString() {
		return "BackupPlan [conf=" + conf + "]";
	}

}
