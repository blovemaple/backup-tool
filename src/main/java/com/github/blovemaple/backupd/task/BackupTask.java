package com.github.blovemaple.backupd.task;

import static com.github.blovemaple.backupd.utils.FileHashing.*;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.util.concurrent.Callable;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.github.blovemaple.backupd.BackupConf;

/**
 * 执行备份的任务，由{@link DetectingTask}生成，负责执行指定的一个文件或目录的备份。
 * 
 * @author blovemaple <blovemaple2010(at)gmail.com>
 */
public class BackupTask implements Callable<Void> {
	private static final Logger logger = Logger.getLogger(BackupTask.class.getSimpleName());

	private final BackupConf conf;
	private final Path relativePath;

	public BackupTask(BackupConf conf, Path relativePath) {
		this.conf = conf;
		this.relativePath = relativePath;
	}

	public Path fromFullPath() {
		return conf.getFromPath().resolve(relativePath);
	}

	public Path toFullPath() {
		return conf.getToPath().resolve(relativePath);
	}

	/**
	 * 判断此任务是否需要执行（是否需要备份，如果from不存在，或两边文件内容相同/目录都存在，则不需要备份）。
	 */
	public boolean needBackup() {
		try {
			Path fromFullPath = fromFullPath();
			Path toFullPath = toFullPath();

			if (Files.notExists(fromFullPath))
				return false;

			if (Files.isDirectory(fromFullPath)) {
				if (!Files.isDirectory(toFullPath))
					return true;

			} else if (Files.isRegularFile(fromFullPath)) {
				if (!Files.isRegularFile(toFullPath))
					return true;

				if (!isEqualFiles(fromFullPath, toFullPath))
					return true;

			}
			return false;
		} catch (Exception e) {
			// 为了保证任务不中止，只打印而不抛出异常
			logger.log(Level.WARNING, "Error checking backup task: " + this, e);
			return false;
		}
	}

	private boolean isEqualFiles(Path file1, Path file2) throws IOException {
		long size1 = Files.size(file1);
		long size2 = Files.size(file1);
		if (size1 != size2)
			return false;

		FileTime time1 = Files.getLastModifiedTime(file1);
		FileTime time2 = Files.getLastModifiedTime(file2);
		if (time1.equals(time2))
			// 为了节约性能，只要文件大小和修改时间都一样，就认为一样，不再比较内容hash
			return true;

		String hash1 = fileMd5(file1);
		String hash2 = fileMd5(file2);
		if (!hash1.equals(hash2))
			return false;

		return true;
	}

	@Override
	public Void call() throws IOException {
		if (!needBackup())
			return null;

		Path fromFullPath = conf.getFromPath().resolve(relativePath);
		Path toFullPath = conf.getToPath().resolve(relativePath);

		if (Files.isDirectory(fromFullPath)) {
			delete(toFullPath);
			Files.createDirectories(toFullPath);

		} else if (Files.isRegularFile(fromFullPath)) {
			delete(toFullPath);
			Files.copy(fromFullPath, toFullPath, StandardCopyOption.COPY_ATTRIBUTES);

		}
		return null;
	}

	private void delete(Path path) throws IOException {
		if (Files.isDirectory(path, LinkOption.NOFOLLOW_LINKS)) {
			Files.walkFileTree(path, new SimpleFileVisitor<Path>() {

				@Override
				public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
					super.visitFile(file, attrs);
					Files.deleteIfExists(file);
					return FileVisitResult.CONTINUE;
				}

				@Override
				public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
					super.postVisitDirectory(dir, exc);
					Files.deleteIfExists(dir);
					return FileVisitResult.CONTINUE;
				}
			});
		} else if (Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)) {
			Files.deleteIfExists(path);
		} else if (Files.isSymbolicLink(path)) {
			Files.deleteIfExists(path);
		}
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((conf == null) ? 0 : conf.hashCode());
		result = prime * result + ((relativePath == null) ? 0 : relativePath.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (!(obj instanceof BackupTask))
			return false;
		BackupTask other = (BackupTask) obj;
		if (conf == null) {
			if (other.conf != null)
				return false;
		} else if (!conf.equals(other.conf))
			return false;
		if (relativePath == null) {
			if (other.relativePath != null)
				return false;
		} else if (!relativePath.equals(other.relativePath))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "BackupTask [conf=" + conf + ", relativePath=" + relativePath + "]";
	}

}
