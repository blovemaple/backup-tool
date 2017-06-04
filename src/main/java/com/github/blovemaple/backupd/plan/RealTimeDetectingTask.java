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
public class RealTimeDetectingTask extends DetectingTask {
	private static final Logger logger = LogManager.getLogger(RealTimeDetectingTask.class);

	public RealTimeDetectingTask(BackupConf conf, BackupDelayingQueue queue) {
		super(conf, queue);
	}

	@Override
	public Void call() throws IOException {
		Path fromPath = conf().getFromPath();

		PathMatcher pathMatcher = conf().newPathMatcher();

		try (WatchService watcher = fromPath.getFileSystem().newWatchService()) {

			Map<WatchKey, Path> pathsByKey = Collections.synchronizedMap(new HashMap<>());

			// 遍历fromPath下所有目录，注册到WatchService。
			Files.walk(fromPath).filter(Files::isDirectory).forEach(rethrowConsumer(path -> {
				WatchKey key = path.register(watcher, ENTRY_CREATE, ENTRY_DELETE, ENTRY_MODIFY);
				pathsByKey.put(key, fromPath.relativize(path));
			}));

			// 循环处理事件
			try {
				while (true) {
					WatchKey eventKey = watcher.take();
					try {
						eventKey.pollEvents().stream().forEach(rethrowConsumer(event -> {
							Path eventPath = (Path) event.context(); // 注册路径到事件路径的相对路径
							Path relativePath = pathsByKey.get(eventKey).resolve(eventPath); // fromPath到事件路径的相对路径

							if (pathMatcher.matches(relativePath))
								submitBackupTask(new BackupTask(conf(), relativePath));

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
			}

		}

		return null;
	}

}
