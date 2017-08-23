package com.github.blovemaple.backupd;

import static org.junit.Assert.*;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;

import org.junit.Test;

import com.github.blovemaple.backupd.task.BackupConf;
import com.github.blovemaple.backupd.task.BackupConf.BackupConfType;
import com.google.common.collect.Lists;

public class ConfGeneratorTest {

	@Test
	public void test() throws IOException {
		List<String> lines = Lists.newArrayList();
		lines.add("backup simple");
		lines.add("from /a/b/c");
		lines.add("to /a/b/d");
		lines.add("backup filter");
		lines.add("from /a/b/c");
		lines.add("to /a/b/d");
		lines.add("only abc");
		lines.add("backup remote");
		lines.add("from remote file:///a/b/c");
		lines.add("to remote   file:///a/b/d");
		lines.add("");
		lines.add("backup comment");
		lines.add(" from /a/b/c");
		lines.add(" ");
		lines.add("to /a/b/d  # comment");
		lines.add("# comment");
		lines.add(" # comment");

		List<BackupConf> confs = ConfGenerator.fromConfLines(lines.stream());

		BackupConf conf0 = confs.get(0);
		assertEquals(conf0.getName(), "simple");
		assertEquals(conf0.getType(), BackupConfType.DAEMON);
		assertEquals(conf0.getFromPath(), Paths.get("/a/b/c"));
		assertEquals(conf0.getToPath(), Paths.get("/a/b/d"));
		assertNull(conf0.getFilter());

		BackupConf conf1 = confs.get(1);
		assertEquals(conf1.getName(), "filter");
		assertEquals(conf1.getType(), BackupConfType.DAEMON);
		assertEquals(conf1.getFromPath(), Paths.get("/a/b/c"));
		assertEquals(conf1.getToPath(), Paths.get("/a/b/d"));
		assertEquals(conf1.getFilter(), "abc");

		BackupConf conf2 = confs.get(2);
		assertEquals(conf2.getName(), "remote");
		assertEquals(conf2.getType(), BackupConfType.DAEMON);
		assertEquals(conf2.getFromPath(), Paths.get("/a/b/c"));
		assertEquals(conf2.getToPath(), Paths.get("/a/b/d"));
		assertNull(conf2.getFilter());

		BackupConf conf3 = confs.get(3);
		assertEquals(conf3.getName(), "comment");
		assertEquals(conf3.getType(), BackupConfType.DAEMON);
		assertEquals(conf3.getFromPath(), Paths.get("/a/b/c"));
		assertEquals(conf3.getToPath(), Paths.get("/a/b/d"));
		assertNull(conf3.getFilter());

	}

}
