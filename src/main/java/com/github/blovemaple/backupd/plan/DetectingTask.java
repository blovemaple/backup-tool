package com.github.blovemaple.backupd.plan;

import java.io.IOException;
import java.util.concurrent.Callable;

import com.github.blovemaple.backupd.machine.BackupDelayingQueue;
import com.github.blovemaple.backupd.machine.ClosedQueueException;

/**
 * 检测任务，实现类实现某种方式检测需要备份的文件并提交给{@link BackupDelayingQueue}。
 * 
 * @author blovemaple <blovemaple2010(at)gmail.com>
 */
public abstract class DetectingTask implements Callable<Void> {
	private final BackupConf conf;
	private final BackupDelayingQueue queue;

	public DetectingTask(BackupConf conf, BackupDelayingQueue queue) {
		this.conf = conf;
		this.queue = queue;
	}

	/**
	 * 子类实现检测逻辑，并调用{@link #submitBackupTask(BackupTask)}提交备份任务。
	 * 
	 * @throws IOException
	 * 
	 * @see java.lang.Runnable#run()
	 */
	@Override
	public abstract Void call() throws IOException;

	/**
	 * 返回使用的配置。
	 */
	protected BackupConf conf() {
		return conf;
	}

	/**
	 * 实现类调用，提交一个备份任务。
	 * 
	 * @param task
	 *            任务
	 * @throws InterruptedException
	 * @throws IOException
	 * @throws ClosedQueueException
	 */
	protected void submitBackupTask(BackupTask task) throws InterruptedException, ClosedQueueException, IOException {
		queue.submit(task);
	}

}
