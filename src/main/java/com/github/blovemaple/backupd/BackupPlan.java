package com.github.blovemaple.backupd;

import java.io.IOException;
import java.nio.file.Files;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.github.blovemaple.backupd.BackupConf.BackupConfType;
import com.github.blovemaple.backupd.task.BackupControllerTask;
import com.github.blovemaple.backupd.task.FullDetectingTask;
import com.github.blovemaple.backupd.task.RealTimeDetectingTask;

/**
 * 备份计划，即执行一个{@link BackupConf}的任务。
 * 
 * @author blovemaple <blovemaple2010(at)gmail.com>
 */
public class BackupPlan implements Runnable {
	private static final Logger logger = Logger.getLogger(BackupPlan.class.getSimpleName());

	private final BackupConf conf;
	private final ExecutorService executor;

	private final BackupDelayingQueue queue = new BackupDelayingQueue();
	private Future<?> backupControl;
	private Future<?> realTimeDetecting, fullDetecting;

	public BackupPlan(BackupConf conf, ExecutorService executor) throws IOException {
		validate(conf);
		this.conf = conf;
		this.executor = executor;
	}

	private void validate(BackupConf conf) throws IOException {
		if (!Files.isReadable(conf.getFromPath()))
			// fromPath不存在或不可读，报错
			throw new IllegalArgumentException("From-path does not exist or is unreadable: " + conf.getFromPath());

		if (Files.isRegularFile(conf.getFromPath()))
			throw new IllegalArgumentException("Cannot backup from a file.");
		if (Files.isRegularFile(conf.getToPath()))
			throw new IllegalArgumentException("Cannot backup into a file.");

		if (conf.getType() == BackupConfType.DAEMON)
			// 尝试开启watchservice，确保可以用
			conf.getFromPath().getFileSystem().newWatchService().close();
	}

	@Override
	public void run() {
		// 开启备份控制任务
		BackupControllerTask bct = new BackupControllerTask(queue);
		backupControl = executor.submit(bct);

		// 如果conf是daemon的，则先开启实时检测
		if (conf.getType() == BackupConfType.DAEMON)
			realTimeDetecting = executor.submit(new RealTimeDetectingTask(conf, queue));

		// 无论daemon还是一次性备份，都要执行完整检测
		fullDetecting = executor.submit(new FullDetectingTask(conf, queue));

		try {
			// 等待完整检测完毕
			fullDetecting.get();
			if (realTimeDetecting != null)
				// hold在实时检测任务上（不会结束，只能等interrupt）
				realTimeDetecting.get();

			// 停止运行备份控制任务
			bct.close();
			try {
				backupControl.get();
			} catch (Exception e) {
				// 再次发生异常，打印异常不再处理
				logger.log(Level.WARNING, "Error occured when waiting for backup controller task to stop.", e);
			}
		} catch (Exception e) {
			// plan执行被interrupt，或某任务发生异常，则强行中止检测、备份控制任务
			backupControl.cancel(true);
			fullDetecting.cancel(true);
			if (realTimeDetecting != null)
				realTimeDetecting.cancel(true);
		}

	}

}
