package com.github.blovemaple.backupd.task;

import java.nio.file.ClosedFileSystemException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.util.Objects;

import com.google.common.base.Strings;

/**
 * 一条备份设置。
 * 
 * @author blovemaple <blovemaple2010(at)gmail.com>
 */
public class BackupConf {
	private String name;
	private Path fromPath;
	private Path toPath;
	private String filter;
	private BackupConfType type;

	public static enum BackupConfType {
		DAEMON, ONCE
	}

	public BackupConf(Path fromPath, Path toPath, BackupConfType type) {
		this.fromPath = fromPath;
		this.toPath = toPath;
		this.type = type;
	}

	public BackupConf(Path fromPath, Path toPath, String filter, BackupConfType type) {
		this.fromPath = fromPath;
		this.toPath = toPath;
		this.filter = filter;
		this.type = type;
	}

	public BackupConf(BackupConfType type) {
		this.type = type;
	}

	public BackupConf() {
	}

	public void validate() throws RuntimeException {
		Objects.requireNonNull(type, "Type is not specified.");
		Objects.requireNonNull(fromPath, "From-path is not specified.");
		Objects.requireNonNull(toPath, "To-path is not specified.");
	}

	public void checkReady() throws BackupConfNotReadyException {
		try {
			if (!Files.isReadable(fromPath))
				// fromPath不存在或不可读
				throw new BackupConfNotReadyException("From-path does not exist or is unreadable: " + fromPath);
		} catch (ClosedFileSystemException e) {
			throw new BackupConfNotReadyException("File system is closed.", e);
		}

		// fromPath和toPath都不能是普通文件
		if (Files.isRegularFile(fromPath))
			throw new BackupConfNotReadyException("Cannot backup from a file.");
		if (Files.isRegularFile(toPath))
			throw new BackupConfNotReadyException("Cannot backup into a file.");

		try {
			if (type == BackupConfType.DAEMON)
				// 尝试给fromPath所在的文件系统开启watchservice，确保可以用
				fromPath.getFileSystem().newWatchService().close();
		} catch (Exception e) {
			throw new BackupConfNotReadyException("From-path is not watchable.", e);
		}
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Path getFromPath() {
		return fromPath;
	}

	public void setFromPath(Path fromPath) {
		this.fromPath = fromPath;
	}

	public Path getToPath() {
		return toPath;
	}

	public void setToPath(Path toPath) {
		this.toPath = toPath;
	}

	public String getFilter() {
		return filter;
	}

	public void setFilter(String filter) {
		this.filter = filter;
	}

	public BackupConfType getType() {
		return type;
	}

	public void setType(BackupConfType type) {
		this.type = type;
	}

	public PathMatcher newPathMatcher() {
		PathMatcher pathMatcher;
		if (Strings.isNullOrEmpty(getFilter())) {
			pathMatcher = anyPath -> true;
		} else {
			pathMatcher = fromPath.getFileSystem().getPathMatcher("glob:" + getFilter());
		}
		return pathMatcher;
	}

	@Override
	public String toString() {
		return "BackupConf [name=" + name + ", fromPath=" + fromPath + ", toPath=" + toPath + ", filter=" + filter
				+ ", type=" + type + "]";
	}

}
