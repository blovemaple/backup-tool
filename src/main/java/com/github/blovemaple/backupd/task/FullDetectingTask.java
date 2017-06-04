package com.github.blovemaple.backupd.task;

import static com.github.blovemaple.backupd.utils.LambdaUtils.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Stream;

import com.github.blovemaple.backupd.BackupConf;
import com.github.blovemaple.backupd.BackupDelayingQueue;
import com.github.blovemaple.backupd.ClosedQueueException;

/**
 * 执行一次完整检测的任务。
 * 
 * @author blovemaple <blovemaple2010(at)gmail.com>
 */
public class FullDetectingTask extends DetectingTask {
	private static final Logger logger = Logger.getLogger(FullDetectingTask.class.getSimpleName());

	public FullDetectingTask(BackupConf conf, BackupDelayingQueue queue) {
		super(conf, queue);
	}

	@Override
	public Void call() throws IOException {
		Path fromPath = conf().getFromPath();
		Path toPath = conf().getToPath();

		if (Files.notExists(fromPath)) {
			return null;
		}

		PathMatcher pathMatcher = conf().newPathMatcher();

		try {
			// 遍历fromPath和toPath下所有的Path，生成所有相对路径
			Stream.concat(Files.walk(fromPath).map(fromPath::relativize), Files.walk(toPath).map(toPath::relativize))
					// 去重
					.distinct()
					// 根据配置过滤
					.filter(pathMatcher::matches)
					// 为每个Path创建BackupTask
					.map(relativePath -> new BackupTask(conf(), relativePath))
					// 过滤出需要备份的task
					.filter(rethrowPredicate(BackupTask::needBackup))
					// 提交到队列
					.forEachOrdered(rethrowConsumer(this::submitBackupTask));
		} catch (IOException e) {
			throw new IOException("Error walking from-path: " + fromPath, e);
		} catch (ClosedQueueException | InterruptedException e) {
			// 队列被关闭或线程被中断，直接结束
		} catch (Exception e) {
			logger.log(Level.SEVERE, "Unknown error in full detecting task of conf " + conf(), e);
		}

		return null;
	}

}
