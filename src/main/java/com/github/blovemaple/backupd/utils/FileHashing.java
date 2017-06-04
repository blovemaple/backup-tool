package com.github.blovemaple.backupd.utils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;

import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;

/**
 * @author blovemaple <blovemaple2010(at)gmail.com>
 */
public class FileHashing {
	/**
	 * 计算文件内容的MD5值。
	 * 
	 * @param filePath
	 *            文件路径
	 * @return MD5字符串
	 * @throws IOException
	 */
	public static String fileMd5(Path filePath) throws IOException {
		Hasher hasher = Hashing.md5().newHasher();
		try (InputStream in = Files.newInputStream(filePath)) {
			byte[] bytes = new byte[8192];
			int len;
			while ((len = in.read(bytes)) >= 0) {
				hasher.putBytes(bytes, 0, len);
			}
		}
		return hasher.toString();
	}
}
