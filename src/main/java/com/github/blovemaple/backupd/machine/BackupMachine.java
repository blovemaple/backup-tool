package com.github.blovemaple.backupd.machine;

import static com.github.blovemaple.backupd.utils.LambdaUtils.*;
import static java.nio.file.StandardWatchEventKinds.*;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.github.blovemaple.backupd.BackupConf;

/**
 * 备份主程序。
 * 
 * @author blovemaple <blovemaple2010(at)gmail.com>
 */
public class BackupMachine implements Closeable {
	private final List<BackupConf> fullDetectingQueue = Collections.synchronizedList(new LinkedList<>());
	private final Set<BackupRequest> requests = Collections.synchronizedSet(new LinkedHashSet<>());

	private final Map<FileSystem, DetectingTask> detectingTasks = new HashMap<>();
	private final FullDetectingTask fullDetectingTask = new FullDetectingTask();
	private final BackupingTask backupingTask = new BackupingTask();

	private final ExecutorService executor = Executors.newCachedThreadPool();
	private boolean closed = false;

	public BackupMachine() {
		executor.submit(backupingTask);
		executor.submit(fullDetectingTask);
	}

	public void doAsDaemon(List<BackupConf> confs) throws IOException {
		if (closed)
			throw new IllegalStateException("Backup machine is already closed.");

		for (BackupConf conf : confs) {
			// 1. 新建或复用同FileSystem的DetectingTask进行监听
			FileSystem fromFS = conf.getFromPath().getFileSystem();
			DetectingTask detectingTask = detectingTasks.get(fromFS);

			if (detectingTask == null) {
				DetectingTask existedTask = detectingTasks.putIfAbsent(fromFS, detectingTask = new DetectingTask());
				if (existedTask == null)
					executor.submit(detectingTask);
				else
					detectingTask = existedTask;
			}

			detectingTask.addConf(conf);

			// 2. 添加进fullDetectingQueue进行一次完全检测
			fullDetectingQueue.add(conf);
		}
	}

	public void doOnce(List<BackupConf> confs) {
		if (closed)
			throw new IllegalStateException("Backup machine is already closed.");

		// 添加进fullDetectingQueue头部，最高优先级
		fullDetectingQueue.addAll(0, confs);
	}

	@Override
	public void close() throws IOException {
		try {
			closed = true;
			executor.shutdownNow();
			boolean shutdownSuccess = executor.awaitTermination(10, TimeUnit.SECONDS);
			if (!shutdownSuccess)
				throw new IOException("Backup machine shutdown timeout.");
		} catch (InterruptedException e) {
			// 已经在关闭，忽略
		}
	}

	private class DetectingTask implements Runnable {
		private final List<BackupConf> confs = new ArrayList<>();

		private final Map<BackupConf, List<WatchPath>> watchPathsByConf = new HashMap<>();
		private final Map<Path, WatchKey> keyByPath = new HashMap<>();
		private final Map<WatchKey, List<WatchPath>> watchPathsByKey = new HashMap<>();

		private FileSystem fileSystem;
		private WatchService watchService;

		public synchronized void addConf(BackupConf conf) throws IOException {
			Path fromPath = conf.getFromPath();
			FileSystem fromFileSystem = fromPath.getFileSystem();
			if (fileSystem == null) {
				fileSystem = fromFileSystem;
				watchService = fileSystem.newWatchService();
				fileSystem = fromFileSystem;
			} else if (fromFileSystem != fileSystem)
				throw new IllegalArgumentException("FileSystem do not match: " + fromFileSystem + ", " + fileSystem);

			confs.add(conf);

			List<WatchPath> watchPathsOfConf = new ArrayList<>();
			watchPathsByConf.put(conf, watchPathsOfConf);

			Files.walk(fromPath).filter(Files::isDirectory).forEach(rethrowConsumer(relativePath -> {
				WatchPath watchPath = new WatchPath(conf, relativePath);

				Path absolutePath = fromPath.resolve(relativePath);
				WatchKey key = keyByPath.get(absolutePath);
				List<WatchPath> watchPathsOfKey;
				if (key == null || !key.isValid()) {
					keyByPath.put(absolutePath,
							key = absolutePath.register(watchService, ENTRY_CREATE, ENTRY_DELETE, ENTRY_MODIFY));
					watchPathsByKey.put(key, watchPathsOfKey = new ArrayList<>());
				} else {
					watchPathsOfKey = watchPathsByKey.get(key);
				}
				watchPathsOfKey.add(watchPath);
				watchPath.setKey(key);

				watchPathsOfConf.add(watchPath);
			}));
		}

		@Override
		public void run() {
			try {
				for (;;) {
					WatchKey key = watchService.take();
					synchronized (DetectingTask.this) {
						List<WatchPath> watchPaths = watchPathsByKey.get(key);
						for (WatchEvent<?> event : key.pollEvents()) {
							Path relativePath = (Path) event.context();
							for (WatchPath watchPath : watchPaths) {
								BackupRequest request = new BackupRequest(watchPath.conf,
										watchPath.relativePath.resolve(relativePath));
								synchronized (requests) {
									requests.remove(request);
									requests.add(request);
								}
							}
						}
						boolean valid = key.reset();
						if (!valid) {
							watchPathsByKey.remove(key);
							WatchPath aWatchPath = watchPaths.get(0);
							Path absolutePath = aWatchPath.conf.getFromPath().resolve(aWatchPath.relativePath);
							keyByPath.remove(absolutePath);
							for (WatchPath watchPath : watchPaths)
								watchPathsByConf.get(watchPath.conf).remove(watchPath);
						}
					}
				}
			} catch (InterruptedException e) {
				// 被中断
			} finally {
				try {
					if (watchService != null)
						watchService.close();
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
			}

		}

	}

	private static class WatchPath {
		private BackupConf conf;
		private Path relativePath;
		@SuppressWarnings("unused")
		private WatchKey key;

		public WatchPath(BackupConf conf, Path relativePath) {
			this.conf = conf;
			this.relativePath = relativePath;
		}

		public void setKey(WatchKey key) {
			this.key = key;
		}
	}

	public static class BackupRequest {
		private BackupConf conf;
		private Path relativePath;

		public BackupRequest(BackupConf conf, Path relativePath) {
			this.conf = conf;
			this.relativePath = relativePath;
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
			if (!(obj instanceof BackupRequest))
				return false;
			BackupRequest other = (BackupRequest) obj;
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
	}

	public static void main1(String[] args) throws IOException, InterruptedException {
		WatchService watcher = FileSystems.getDefault().newWatchService();
		Paths.get("/home/blove/tmp/test").register(watcher, ENTRY_CREATE, ENTRY_DELETE, ENTRY_MODIFY);
		for (;;) {
			WatchKey key = watcher.take();
			for (WatchEvent<?> event : key.pollEvents()) {
				System.out.println(event.kind() + "\t" + event.count() + "\t" + event.context());
			}
			boolean valid = key.reset();
			if (!valid) {
				System.out.println("over");
			}
		}
	}

	private class FullDetectingTask implements Runnable {

		@Override
		public void run() {
			// TODO Auto-generated method stub

		}

	}

	private class BackupingTask implements Runnable {

		@Override
		public void run() {
			// TODO Auto-generated method stub

		}

	}
}
