package com.github.blovemaple.backupd.machine;

/**
 * 当{@link BackupDelayingQueue}已经关闭提交，再次调用提交时抛出此异常。
 * 
 * @author blovemaple <blovemaple2010(at)gmail.com>
 */
public class ClosedQueueException extends RuntimeException {
	private static final long serialVersionUID = 3591132036378144796L;

	public ClosedQueueException() {
		super("Queue entrance is closed.");
	}

}
