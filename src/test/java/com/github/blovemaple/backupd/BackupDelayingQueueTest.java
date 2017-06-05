package com.github.blovemaple.backupd;

import static com.github.blovemaple.backupd.plan.BackupConf.BackupConfType.*;
import static org.junit.Assert.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.util.HashMap;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.github.blovemaple.backupd.machine.BackupDelayingQueue;
import com.github.blovemaple.backupd.plan.BackupConf;
import com.github.blovemaple.backupd.plan.BackupTask;

public class BackupDelayingQueueTest extends TestBase {
	private BackupDelayingQueue queue;

	@Before
	public void setUp1() throws Exception {
		queue = new BackupDelayingQueue(new HashMap<>());
	}

	@After
	public void tearDown1() throws Exception {
		queue.close();
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

	@Test
	public void testRequeue() throws Exception {
		Path fileRequeue = fs.getPath("/org/requeue.file");
		Files.createFile(fileRequeue);

		Path file = fs.getPath("/org/file");
		Files.createFile(file);
		Files.setLastModifiedTime(file, FileTime.fromMillis(System.currentTimeMillis() - 1000));

		BackupConf conf = new BackupConf(fs.getPath("/org"), fs.getPath("/dst"), ONCE);

		@SuppressWarnings("unused")
		BackupTask requeueTask = submit(conf, "requeue.file");
		BackupTask fileTask = submit("file");

		Files.setLastModifiedTime(fileRequeue, FileTime.fromMillis(System.currentTimeMillis() - 2000));
		BackupTask requeueTask1 = submit(conf, "requeue.file");

		assertEquals(requeueTask1, queue.fetch(3));
		assertEquals(fileTask, queue.fetch(3));
		assertNull(queue.fetch(2));
	}

	private BackupTask submit(String fileName) throws InterruptedException, IOException {
		BackupConf conf = new BackupConf(fs.getPath("/org"), fs.getPath("/dst"), ONCE);
		return submit(conf, fileName);
	}

	private BackupTask submit(BackupConf conf, String fileName) throws InterruptedException, IOException {
		BackupTask task = new BackupTask(conf, fs.getPath(fileName));
		queue.submit(task);
		return task;
	}

}
