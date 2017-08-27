package com.github.blovemaple.backupd.task;

/**
 * @author blovemaple <blovemaple2010(at)gmail.com>
 */
public class BackupConfNotReadyException extends IllegalStateException {
	private static final long serialVersionUID = 1L;

	public BackupConfNotReadyException() {
		super();
	}

	public BackupConfNotReadyException(String message, Throwable cause) {
		super(message, cause);
	}

	public BackupConfNotReadyException(String s) {
		super(s);
	}

	public BackupConfNotReadyException(Throwable cause) {
		super(cause);
	}

}
