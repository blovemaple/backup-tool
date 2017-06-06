package com.github.blovemaple.backupd.plan;

import static com.github.blovemaple.backupd.utils.LambdaUtils.*;
import static java.nio.file.StandardWatchEventKinds.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.github.blovemaple.backupd.machine.BackupDelayingQueue;

/**
 * 持续进行实时检测的任务。
 * 
 * @author blovemaple <blovemaple2010(at)gmail.com>
 */
public class RealTimeDetectingTask implements Runnable {
	private static final Logger logger = LogManager.getLogger(RealTimeDetectingTask.class);

	private final BackupConf conf;
	private final BackupDelayingQueue queue;

	public RealTimeDetectingTask(BackupConf conf, BackupDelayingQueue queue) {
		this.conf = conf;
		this.queue = queue;
	}

	@Override
	public void run() {
		logger.info(() -> "Started real time detecting for " + conf);

		Path fromPath = conf.getFromPath();

		PathMatcher pathMatcher = conf.newPathMatcher();

		try (WatchService watcher = fromPath.getFileSystem().newWatchService()) {

			Map<WatchKey, Path> pathsByKey = Collections.synchronizedMap(new HashMap<>());

			// 遍历fromPath下所有目录，注册到WatchService。
			Files.walk(fromPath).filter(Files::isDirectory).forEach(rethrowConsumer(path -> {
				WatchKey key = path.register(watcher, ENTRY_CREATE, ENTRY_DELETE, ENTRY_MODIFY);
				pathsByKey.put(key, fromPath.relativize(path));
			}));

			// 循环处理事件
			while (true) {
				WatchKey eventKey = watcher.take();
				try {
					eventKey.pollEvents().stream().forEach(rethrowConsumer(event -> {
						Path eventPath = (Path) event.context(); // 注册路径到事件路径的相对路径
						Path relativePath = pathsByKey.get(eventKey).resolve(eventPath); // fromPath到事件路径的相对路径

						if (pathMatcher.matches(relativePath))
							queue.submit(new BackupTask(conf, relativePath));

						if (event.kind() == ENTRY_CREATE) {
							Path newPath = fromPath.resolve(relativePath); // 绝对路径
							if (Files.isDirectory(newPath)) {
								WatchKey key = newPath.register(watcher, ENTRY_CREATE, ENTRY_DELETE, ENTRY_MODIFY);
								pathsByKey.put(key, relativePath);
							}
						}
					}));
				} catch (Exception e) {
					// 为了保证任务不中止，只打印而不抛出异常
					logger.error(() -> "Error handling event of path: " + pathsByKey.get(eventKey), e);
				}
				boolean isStillValid = eventKey.reset();
				if (!isStillValid)
					pathsByKey.remove(eventKey);
			}

		} catch (InterruptedException e) {
			// 线程被中断，直接结束
		} catch (IOException e) {
			logger.error(() -> "IO error in real-time detecting task of conf " + conf, e);
		} catch (Exception e) {
			logger.error(() -> "Unknown error in real-time detecting task of conf " + conf, e);
		}

		logger.info(() -> "Ended real time detecting for " + conf);
	}

}
