package com.github.blovemaple.backupd;

import java.nio.file.Path;
import java.nio.file.PathMatcher;

import com.google.common.base.Strings;

/**
 * 一条备份设置。
 * 
 * @author blovemaple <blovemaple2010(at)gmail.com>
 */
public class BackupConf {
	private Path fromPath;
	private Path toPath;
	private String filter;
	private BackupConfType type;

	public static enum BackupConfType {
		DAEMON, ONCE
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
		return "BackupConf [fromPath=" + fromPath + ", toPath=" + toPath + ", filter=" + filter + ", type=" + type
				+ "]";
	}

}
