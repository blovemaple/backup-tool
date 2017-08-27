package com.github.blovemaple.backupd;

import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import com.github.blovemaple.backupd.machine.BackupDelayingQueue;
import com.github.blovemaple.backupd.task.DetectingTask;
import com.github.blovemaple.backupd.task.RealTimeDetectingTask;
import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import com.google.common.jimfs.WatchServiceConfiguration;

public class TestBase {
	protected static FileSystem fs;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		// 指定延迟备份时间、ready等待时间、事件提取时间，测试case依赖这些时间
		BackupDelayingQueue.DELAY_SECONDS = 3;
		DetectingTask.READY_WAITING_SECONDS = 1;
		RealTimeDetectingTask.EVENT_POLL_SECONDS = 1;
		// 测试开始时创建JimFS内存文件系统
		// JimFS使用polling方式进行watching，默认间隔5秒太长了，会导致测试case跑得太慢。改成1秒。
		fs = Jimfs.newFileSystem(Configuration.unix().toBuilder()
				.setWatchServiceConfiguration(WatchServiceConfiguration.polling(1, TimeUnit.SECONDS)).build());
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		// 测试全部结束后关闭JimFS
		fs.close();
	}

	@Before
	public void setUp() throws Exception {
		// 每个case开始前创建frompath，就不用case自己创建了
		Files.createDirectories(fs.getPath("/org"));
	}

	@After
	public void tearDown() throws Exception {
		// 每个case结束后清空JimFS
		clearFs();
	}

	protected void clearFs() throws IOException {
		Files.walkFileTree(fs.getPath("/"), new SimpleFileVisitor<Path>() {
			@Override
			public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
				super.visitFile(file, attrs);
				Files.deleteIfExists(file);
				return FileVisitResult.CONTINUE;
			}

			@Override
			public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
				if (dir.equals(dir.getRoot()))
					return FileVisitResult.CONTINUE;

				super.postVisitDirectory(dir, exc);
				Files.deleteIfExists(dir);
				return FileVisitResult.CONTINUE;
			}
		});
	}
}
