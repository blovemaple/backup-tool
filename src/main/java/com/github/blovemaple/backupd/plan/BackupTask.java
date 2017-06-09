package com.github.blovemaple.backupd.plan;

import static com.github.blovemaple.backupd.utils.FileHashing.*;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.NotDirectoryException;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.util.concurrent.Callable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * 执行备份的任务，由{@link DetectingTask}生成，负责执行指定的一个文件或目录的备份。
 * 
 * @author blovemaple <blovemaple2010(at)gmail.com>
 */
public class BackupTask implements Callable<Boolean> {
	private static final Logger logger = LogManager.getLogger(BackupTask.class);

	private final BackupConf conf;
	private final Path relativePath;

	public BackupTask(BackupConf conf, Path relativePath) {
		this.conf = conf;
		this.relativePath = relativePath;
	}

	public BackupConf conf() {
		return conf;
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

			if (Files.notExists(fromFullPath)) {
				// 若源文件不存在，则不删除目标文件
				return false;
			}

			if (fromFullPath.startsWith(conf.getToPath())) {
				// 如果源文件在toPath内部，则不进行备份，以免无限循环
				return false;
			}

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
			logger.error(() -> "Error checking backup task: " + this, e);
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

		String hash1 = fileHash(file1);
		String hash2 = fileHash(file2);
		if (!hash1.equals(hash2))
			return false;

		return true;
	}

	@Override
	public Boolean call() throws IOException {
		if (!needBackup())
			return false;

		if (Files.notExists(conf.getToPath())) {
			Files.createDirectories(conf.getToPath());
		} else if (!Files.isDirectory(conf.getToPath())) {
			throw new NotDirectoryException(conf.getToPath().toString());
		}

		Path fromFullPath = conf.getFromPath().resolve(relativePath);
		Path toFullPath = conf.getToPath().resolve(relativePath);

		delete(toFullPath);
		prepareParent(toFullPath);
		if (Files.isDirectory(fromFullPath))
			Files.createDirectories(toFullPath);
		else if (Files.isRegularFile(fromFullPath))
			Files.copy(fromFullPath, toFullPath, StandardCopyOption.COPY_ATTRIBUTES);

		return true;
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

	private void prepareParent(Path fullPath) throws IOException {
		Path parent = fullPath.getParent();
		if (parent == null)
			return;
		if (Files.notExists(parent)) {
			Files.createDirectories(parent);
		} else if (!Files.isDirectory(parent)) {
			Files.deleteIfExists(parent);
			Files.createDirectory(parent);
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
