package com.github.blovemaple.backupd.task;

import static com.github.blovemaple.backupd.utils.LambdaUtils.*;
import static java.nio.file.StandardWatchEventKinds.*;

import java.io.IOException;
import java.nio.file.ClosedWatchServiceException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

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

	public static Integer EVENT_POLL_SECONDS = 3;

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
				WatchKey eventKey = null;

				// 在poll事件的同时，每隔一段时间就检查一下conf是否ready，如果不ready则及时退出，交给DetectingTask处理
				while (eventKey == null) {
					eventKey = watcher.poll(EVENT_POLL_SECONDS, TimeUnit.SECONDS);
					try {
						conf.checkReady();
					} catch (Exception e) {
						logger.error(() -> "Backup conf is no longer ready(" + e.getLocalizedMessage() + "): " + conf);
						return;
					}
				}

				WatchKey realEventKey = eventKey;

				// 取出事件并处理
				try {
					realEventKey.pollEvents().forEach(rethrowConsumer(event -> {
						logger.debug(() -> "New event: " + event);

						Path eventPath = (Path) event.context(); // 注册路径到事件路径的相对路径
						Path relativePath = pathsByKey.get(realEventKey).resolve(eventPath); // fromPath到事件路径的相对路径

						if (pathMatcher.matches(relativePath))
							queue.submit(new BackupTask(conf, relativePath));

						if (event.kind() == ENTRY_CREATE) {
							Path newPath = fromPath.resolve(relativePath); // 绝对路径
							if (Files.isDirectory(newPath)) {
								WatchKey key = newPath.register(watcher, ENTRY_CREATE, ENTRY_DELETE, ENTRY_MODIFY);
								pathsByKey.put(key, relativePath);
								// 新目录注册到watcher后进行一次全量检测，以免此前新目录内已经新建了子目录或文件而漏掉
								fullDetect(newPath);
							}
						}
					}));
				} catch (Exception e) {
					// 为了保证任务不中止，只打印而不抛出异常
					logger.error(() -> "Error handling event of path: " + pathsByKey.get(realEventKey), e);
				}
				boolean isStillValid = realEventKey.reset();
				if (!isStillValid)
					pathsByKey.remove(realEventKey);
			}

		} catch (InterruptedException e) {
			// 线程被中断，直接结束
		} catch (ClosedWatchServiceException e) {
			// WatchService被关闭，直接结束
		} catch (IOException e) {
			logger.error(() -> "IO error in real-time detecting task of conf " + conf, e);
		} catch (Exception e) {
			logger.error(() -> "Unknown error in real-time detecting task of conf " + conf, e);
		} finally {
			logger.info(() -> "Ended real time detecting for " + conf);
		}
	}

	private void fullDetect(Path fullPath) throws IOException, RuntimeException, Exception {
		PathMatcher pathMatcher = conf.newPathMatcher();
		Files.walk(fullPath).map(conf.getFromPath()::relativize)
				// 去掉dir本身
				.filter(path -> !path.toString().isEmpty())
				// 去重
				.distinct()
				// 根据配置过滤
				.filter(pathMatcher::matches)
				// 为每个Path创建BackupTask
				.map(relativePath -> new BackupTask(conf, relativePath))
				// 过滤出需要备份的task
				.filter(rethrowPredicate(BackupTask::needBackup))
				// 提交到队列
				.forEachOrdered(rethrowConsumer(queue::submit));
	}

}
