package com.github.blovemaple.backupd;

import static com.github.blovemaple.backupd.plan.BackupConf.BackupConfType.*;
import static com.github.blovemaple.backupd.utils.FileHashing.*;
import static org.junit.Assert.*;

import java.io.IOException;
import java.nio.file.Files;

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

	@Test
	public void testFullOnce() throws Exception {
		BackupConf conf = new BackupConf(fs.getPath("/org"), fs.getPath("/dst"), ONCE);
		BackupMonitor monitor = machine.execute(conf);
		monitor.get();
		assertSuccess();
	}

	private void assertSuccess() throws IOException {
		assertEqualFiles("file1");
		assertEqualFiles("file2");
		assertEqualFiles("dir1");
		assertEqualFiles("dir1/file11");
		assertEqualFiles("dir1/file12");
		assertEqualFiles("dir1/dir11");
		assertEqualFiles("dir1/dir11/file111");
		assertEqualFiles("dir1/dir11/file112");
	}

	private void assertEqualFiles(String relativePath) throws IOException {
		if (Files.isRegularFile(fs.getPath("/org/" + relativePath))) {
			String orgHash = fileHash(fs.getPath("/org/" + relativePath));
			String dstHash = fileHash(fs.getPath("/dst/" + relativePath));
			assertEquals(orgHash, dstHash);
		} else if (Files.isDirectory(fs.getPath("/org/" + relativePath))) {
			assertTrue(Files.isDirectory(fs.getPath("/dst/" + relativePath)));
		} else {
			fail("May not exists? " + fs.getPath("/org/" + relativePath));
		}

	}
}
