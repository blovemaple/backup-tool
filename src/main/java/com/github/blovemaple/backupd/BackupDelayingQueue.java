package com.github.blovemaple.backupd;

import com.github.blovemaple.backupd.task.BackupTask;

/**
 * 检测任务提交的文件等待执行备份的队列。每个计划有一个。 TODO 总共用一个
 * 
 * @author blovemaple <blovemaple2010(at)gmail.com>
 */
public class BackupDelayingQueue {
	/**
	 * 提交一个备份任务。
	 * 
	 * @param task
	 *            任务
	 */
	public void submit(BackupTask task) throws InterruptedException {
		// TODO
	}

	/**
	 * 取出一个备份任务，如无任务可取则等待直到取出或超时。
	 * 
	 * @param waitingSeconds
	 *            等待秒数
	 * @return 备份任务，超时返回null
	 */
	public BackupTask fetch(int waitingSeconds) throws InterruptedException {
		// TODO
		return null;
	}

	/**
	 * 停止提交。调用后再提交会抛出异常{@link ClosedQueueException}。
	 */
	public void closeEntrance() {
		// TODO
	}
}
