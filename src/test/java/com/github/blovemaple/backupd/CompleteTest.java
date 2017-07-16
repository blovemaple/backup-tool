package com.github.blovemaple.backupd;

import static com.github.blovemaple.backupd.task.BackupConf.BackupConfType.*;
import static com.github.blovemaple.backupd.utils.FileHashing.*;
import static org.junit.Assert.*;
import static com.github.blovemaple.backupd.utils.LambdaUtils.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.github.blovemaple.backupd.machine.BackupMachine;
import com.github.blovemaple.backupd.machine.BackupMonitor;
import com.github.blovemaple.backupd.task.BackupConf;

public class CompleteTest extends TestBase {
	private static BackupMachine machine;

	@BeforeClass
	public static void setUpBeforeClass1() throws Exception {
		machine = new BackupMachine();
	}

	@Before
	public void setUp1() throws Exception {
		Files.createFile(fs.getPath("/org/file1"));
		Files.createFile(fs.getPath("/org/file2"));
		Files.createDirectories(fs.getPath("/org/dir1"));
		Files.createFile(fs.getPath("/org/dir1/file11"));
		Files.createFile(fs.getPath("/org/dir1/file12"));
		Files.createDirectories(fs.getPath("/org/dir1/dir11"));
		Files.createFile(fs.getPath("/org/dir1/dir11/file111"));
		Files.createFile(fs.getPath("/org/dir1/dir11/file112"));
		Files.createDirectories(fs.getPath("/org/dir1/dir12"));
	}

	// @Test
	public void testFullOnce() throws Exception {
		BackupConf conf = new BackupConf(fs.getPath("/org"), fs.getPath("/dst"), ONCE);
		BackupMonitor monitor = machine.execute(conf);
		monitor.get();
		assertSuccess();

		monitor.cancel(true);
	}

	@Test
	public void testIncrOnce() throws Exception {
		testFullOnce();

		Files.delete(fs.getPath("/org/dir1/dir11/file111"));
		Files.delete(fs.getPath("/org/dir1/dir11/file112"));
		Files.delete(fs.getPath("/org/dir1/dir11"));

		Files.createDirectories(fs.getPath("/org/dir2"));
		Files.createFile(fs.getPath("/org/dir2/file21"));
		Files.createFile(fs.getPath("/org/dir2/file22"));

		Files.write(fs.getPath("/org/dir1/file11"), Arrays.asList("123", "abc"));

		BackupConf conf = new BackupConf(fs.getPath("/org"), fs.getPath("/dst"), ONCE);
		BackupMonitor monitor = machine.execute(conf);
		monitor.get();

		assertSuccess();

		monitor.cancel(true);
	}

	@Test
	public void testFilterOnce() throws Exception {
		BackupConf conf = new BackupConf(fs.getPath("/org"), fs.getPath("/dst"), "**/*11", ONCE);
		BackupMonitor monitor = machine.execute(conf);
		monitor.get();
		assertFilterSuccess();

		monitor.cancel(true);
	}

	@Test
	public void testFullRealtime() throws Exception {
		BackupConf conf = new BackupConf(fs.getPath("/org"), fs.getPath("/dst"), DAEMON);
		BackupMonitor monitor = machine.execute(conf);
		TimeUnit.SECONDS.sleep(4);
		assertSuccess();

		monitor.cancel(true);
	}

	@Test
	public void testIncrRealtime() throws Exception {
		BackupConf conf = new BackupConf(fs.getPath("/org"), fs.getPath("/dst"), DAEMON);
		BackupMonitor monitor = machine.execute(conf);

		TimeUnit.SECONDS.sleep(4);

		Files.delete(fs.getPath("/org/dir1/dir11/file111"));
		Files.delete(fs.getPath("/org/dir1/dir11/file112"));
		Files.delete(fs.getPath("/org/dir1/dir11"));

		Files.createDirectories(fs.getPath("/org/dir3"));
		Files.createDirectories(fs.getPath("/org/dir3/dir21"));
		Files.createFile(fs.getPath("/org/dir3/file21"));
		Files.createFile(fs.getPath("/org/dir3/file22"));

		Files.write(fs.getPath("/org/dir1/file11"), Arrays.asList("123", "abc"));
		TimeUnit.SECONDS.sleep(4);

		assertSuccess();

		monitor.cancel(true);
	}

	@Test
	public void testFastModRealtime() throws Exception {
		BackupConf conf = new BackupConf(fs.getPath("/org"), fs.getPath("/dst"), DAEMON);
		BackupMonitor monitor = machine.execute(conf);

		TimeUnit.SECONDS.sleep(4);

		for (int i = 0; i < 3; i++) {
			Files.write(fs.getPath("/org/dir1/file11"), Arrays.asList("123" + i, "abc"));
			TimeUnit.SECONDS.sleep(1);
		}
		TimeUnit.SECONDS.sleep(3);

		assertSuccess();

		monitor.cancel(true);
	}

	@Test
	public void testFilterRealtime() throws Exception {
		BackupConf conf = new BackupConf(fs.getPath("/org"), fs.getPath("/dst"), "**/*11", DAEMON);
		BackupMonitor monitor = machine.execute(conf);

		TimeUnit.SECONDS.sleep(4);

		Files.delete(fs.getPath("/org/dir1/dir11/file111"));
		Files.delete(fs.getPath("/org/dir1/dir11/file112"));
		Files.delete(fs.getPath("/org/dir1/dir11"));

		Files.createDirectories(fs.getPath("/org/dir3"));
		Files.createDirectories(fs.getPath("/org/dir3/dir21"));
		Files.createFile(fs.getPath("/org/dir3/file21"));
		Files.createFile(fs.getPath("/org/dir3/file22"));

		Files.write(fs.getPath("/org/dir1/file11"), Arrays.asList("123", "abc"));

		TimeUnit.SECONDS.sleep(4);

		assertFilterSuccess();

		monitor.cancel(true);
	}

	@Test
	public void testDeletingIgnored() throws Exception {
		testFullOnce();

		Files.delete(fs.getPath("/org/dir1/dir11/file111"));
		Files.delete(fs.getPath("/org/dir1/dir11/file112"));
		Files.delete(fs.getPath("/org/dir1/dir12"));

		BackupConf conf = new BackupConf(fs.getPath("/org"), fs.getPath("/dst"), ONCE);
		BackupMonitor monitor = machine.execute(conf);
		monitor.get();

		assertTrue(Files.exists(fs.getPath("/dst/dir1/dir11/file111")));
		assertTrue(Files.exists(fs.getPath("/dst/dir1/dir11/file112")));
		assertTrue(Files.exists(fs.getPath("/dst/dir1/dir12")));

		monitor.cancel(true);
	}

	@Test
	public void testAvoidingLoop() throws Exception {
		Path dst = fs.getPath("/org/dir111");

		BackupConf conf = new BackupConf(fs.getPath("/org"), dst, DAEMON);
		BackupMonitor monitor = machine.execute(conf);
		TimeUnit.SECONDS.sleep(8);
		assertTrue(Files.notExists(fs.getPath("/org/dir111/dir111")));

		monitor.cancel(true);
	}

	private void assertSuccess() throws IOException {
		Path org = fs.getPath("/org");
		Files.walk(org).map(org::relativize).forEach(rethrowConsumer(this::assertEqualFiles));
	}

	private void assertFilterSuccess() throws IOException {
		Path org = fs.getPath("/org");
		Files.walk(org).map(org::relativize).filter(path -> !path.toString().isEmpty())
				.forEach(rethrowConsumer(path -> {
					if ("dir1".equals(path.getFileName().toString()) || path.getFileName().toString().matches(".*11$"))
						assertEqualFiles(path);
					else
						assertTrue(Files.notExists(fs.getPath("/dst").resolve(path)));
				}));
	}

	private void assertEqualFiles(Path relativePath) throws IOException {
		assertEqualFiles(relativePath, fs.getPath("/dst"));
	}

	private void assertEqualFiles(Path relativePath, Path dst) throws IOException {
		Path org = fs.getPath("/org");
		if (Files.isRegularFile(org.resolve(relativePath))) {
			String orgHash = fileHash(org.resolve(relativePath));
			String dstHash = fileHash(dst.resolve(relativePath));
			assertEquals(orgHash, dstHash);
		} else if (Files.isDirectory(org.resolve(relativePath))) {
			assertTrue(Files.isDirectory(dst.resolve(relativePath)));
		} else {
			fail("May not exists? " + org.resolve(relativePath));
		}
	}
}
