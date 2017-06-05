package com.github.blovemaple.backupd;

import static com.github.blovemaple.backupd.plan.BackupConf.BackupConfType.*;
import static org.junit.Assert.*;
import static com.github.blovemaple.backupd.utils.FileHashing.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

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
		if (Files.isRegularFile(Paths.get("/org/" + relativePath))) {
			String orgHash = fileHash(Paths.get("/org/" + relativePath));
			String dstHash = fileHash(Paths.get("/dst/" + relativePath));
			assertEquals(orgHash, dstHash);
		} else if (Files.isDirectory(Paths.get("/org/" + relativePath))) {
			assertTrue(Files.isDirectory(Paths.get("/dst/" + relativePath)));
		} else {
			fail("May not exists? " + Paths.get("/org/" + relativePath));
		}

	}
}
