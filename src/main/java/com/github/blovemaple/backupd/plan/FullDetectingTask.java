package com.github.blovemaple.backupd.plan;

import static com.github.blovemaple.backupd.utils.LambdaUtils.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.util.stream.Stream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.github.blovemaple.backupd.machine.BackupDelayingQueue;

/**
 * 执行一次完整检测的任务。
 * 
 * @author blovemaple <blovemaple2010(at)gmail.com>
 */
public class FullDetectingTask implements Runnable {
	private static final Logger logger = LogManager.getLogger(FullDetectingTask.class);

	private final BackupConf conf;
	private final BackupDelayingQueue queue;

	public FullDetectingTask(BackupConf conf, BackupDelayingQueue queue) {
		this.conf = conf;
		this.queue = queue;
	}

	@Override
	public void run() {
		logger.info(() -> "Started full detecting for " + conf);

		Path fromPath = conf.getFromPath();
		Path toPath = conf.getToPath();

		if (Files.notExists(fromPath)) {
			return;
		}

		PathMatcher pathMatcher = conf.newPathMatcher();

		try {
			// 遍历fromPath和toPath下所有的Path，生成所有相对路径
			Stream<Path> pathStream = Files.walk(fromPath).map(fromPath::relativize);
			if (Files.isDirectory(toPath))
				pathStream = Stream.concat(pathStream, Files.walk(toPath).map(toPath::relativize));

			pathStream
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
		} catch (InterruptedException e) {
			// 线程被中断，直接结束
		} catch (IOException e) {
			logger.error(() -> "IO error in full detecting task of conf " + conf, e);
		} catch (Exception e) {
			logger.error(() -> "Unknown error in full detecting task of conf " + conf, e);
		}

		logger.info(() -> "Ended full detecting for " + conf);
	}

}
