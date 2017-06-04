package com.github.blovemaple.backupd.plan;

import java.io.IOException;
import java.nio.file.Files;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.github.blovemaple.backupd.machine.BackupDelayingQueue;
import com.github.blovemaple.backupd.plan.BackupConf.BackupConfType;

/**
 * 备份计划，即执行一个{@link BackupConf}的任务。
 * 
 * @author blovemaple <blovemaple2010(at)gmail.com>
 */
public class BackupPlan implements Runnable {
	private static final Logger logger = LogManager.getLogger(BackupPlan.class);

	private final BackupConf conf;
	private final BackupDelayingQueue queue;

	public BackupPlan(BackupConf conf, BackupDelayingQueue queue) throws IOException {
		validate(conf);
		this.conf = conf;
		this.queue = queue;
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
		ExecutorService executor = Executors.newCachedThreadPool();
		Future<?> realTimeDetecting = null, fullDetecting;

		try {
			// 如果conf是daemon的，则先开启实时检测
			if (conf.getType() == BackupConfType.DAEMON)
				realTimeDetecting = executor.submit(new RealTimeDetectingTask(conf, queue));

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
		}
	}

	@Override
	public String toString() {
		return "BackupPlan [conf=" + conf + "]";
	}

}
