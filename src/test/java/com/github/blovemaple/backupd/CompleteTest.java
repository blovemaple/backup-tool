package com.github.blovemaple.backupd;

import static com.github.blovemaple.backupd.plan.BackupConf.BackupConfType.*;
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
import com.github.blovemaple.backupd.plan.BackupConf;

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
	}

	// @Test
	public void testFullOnce() throws Exception {
		BackupConf conf = new BackupConf(fs.getPath("/org"), fs.getPath("/dst"), ONCE);
		BackupMonitor monitor = machine.execute(conf);
		monitor.get();
		assertSuccess();
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
	}

	@Test
	public void testFilterOnce() throws Exception {
		// TODO
	}

	@Test
	public void testFullRealtime() throws Exception {
		BackupConf conf = new BackupConf(fs.getPath("/org"), fs.getPath("/dst"), DAEMON);
		machine.execute(conf);
		TimeUnit.SECONDS.sleep(4);
		assertSuccess();
	}

	@Test
	public void testIncrRealtime() throws Exception {
		testFullOnce();

		BackupConf conf = new BackupConf(fs.getPath("/org"), fs.getPath("/dst"), DAEMON);
		machine.execute(conf);

		TimeUnit.SECONDS.sleep(1);

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
	}

	@Test
	public void testFastModRealtime() throws Exception {
		// TODO
	}

	@Test
	public void testFilterRealtime() throws Exception {
		// TODO
	}

	private void assertSuccess() throws IOException {
		Path org = fs.getPath("/org");
		Files.walk(org).map(org::relativize).forEach(rethrowConsumer(this::assertEqualFiles));
	}

	private void assertEqualFiles(Path relativePath) throws IOException {
		Path org = fs.getPath("/org");
		Path dst = fs.getPath("/dst");
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
