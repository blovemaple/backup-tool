package com.github.blovemaple.backupd;

import static com.github.blovemaple.backupd.plan.BackupConf.BackupConfType.*;
import static org.junit.Assert.*;

import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.github.blovemaple.backupd.machine.BackupDelayingQueue;
import com.github.blovemaple.backupd.plan.BackupConf;
import com.github.blovemaple.backupd.plan.BackupTask;
import com.google.common.jimfs.Jimfs;

public class BackupDelayingQueueTest {
	private static FileSystem fs;
	private BackupDelayingQueue queue;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		BackupDelayingQueue.DELAY_SECONDS = 3;
		fs = Jimfs.newFileSystem();
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		fs.close();
	}

	@Before
	public void setUp() throws Exception {
		queue = new BackupDelayingQueue();
		Files.createDirectories(fs.getPath("/org"));
	}

	@After
	public void tearDown() throws Exception {
		queue.close();
		deleteDir(fs.getPath("/org"));
		deleteDir(fs.getPath("/dst"));
	}

	private void deleteDir(Path path) throws IOException {
		if (Files.notExists(path))
			return;
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
	}

	@Test
	public void testNoDelay() throws Exception {
		Path file = fs.getPath("/org/file");
		Files.createFile(file);
		Files.setLastModifiedTime(file, FileTime.fromMillis(System.currentTimeMillis() - 3000));

		BackupTask task = submit("file");

		assertEquals(task, queue.fetch(0));
	}

	@Test
	public void testDelay() throws Exception {
		Path file = fs.getPath("/org/file");
		Files.createFile(file);

		BackupTask task = submit("file");

		assertNull(queue.fetch(0));
		assertNull(queue.fetch(2));
		assertEquals(task, queue.fetch(3));
	}

	@Test
	public void testSort() throws Exception {
		Path fileDelayLong = fs.getPath("/org/long.file");
		Files.createFile(fileDelayLong);

		Path fileDelayShort = fs.getPath("/org/short.file");
		Files.createFile(fileDelayShort);
		Files.setLastModifiedTime(fileDelayShort, FileTime.fromMillis(System.currentTimeMillis() - 2000));

		Path fileReady = fs.getPath("/org/ready.file");
		Files.createFile(fileReady);
		Files.setLastModifiedTime(fileReady, FileTime.fromMillis(System.currentTimeMillis() - 3000));

		BackupTask delayLongTask = submit("long.file");
		BackupTask delayShortTask = submit("short.file");
		BackupTask readyTask = submit("ready.file");

		assertEquals(readyTask, queue.fetch(3));
		assertEquals(delayShortTask, queue.fetch(3));
		assertEquals(delayLongTask, queue.fetch(3));
	}

	private BackupTask submit(String fileName) throws InterruptedException, IOException {
		BackupConf conf = new BackupConf(fs.getPath("/org"), fs.getPath("/dst"), ONCE);
		BackupTask task = new BackupTask(conf, fs.getPath(fileName));
		queue.submit(task);
		return task;
	}

}
