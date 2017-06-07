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
import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import com.google.common.jimfs.WatchServiceConfiguration;

public class TestBase {
	protected static FileSystem fs;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		BackupDelayingQueue.DELAY_SECONDS = 3;
		fs = Jimfs.newFileSystem(Configuration.unix().toBuilder()
				.setWatchServiceConfiguration(WatchServiceConfiguration.polling(1, TimeUnit.SECONDS)).build());
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		fs.close();
	}

	@Before
	public void setUp() throws Exception {
		Files.createDirectories(fs.getPath("/org"));
	}

	@After
	public void tearDown() throws Exception {
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
}
