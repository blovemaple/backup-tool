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
	 * 计算文件内容的哈希值（murmur3_128）。
	 * 
	 * @param filePath
	 *            文件路径
	 * @return 哈希值16进制字符串
	 * @throws IOException
	 */
	public static String fileHash(Path filePath) throws IOException {
		// guava的Hashing不建议用md5（慢），快速hash建议用goodFastHash，
		// 但goodFastHash每次加载使用随机种子，导致结果不固定。固定结果的hash建议用murmur3_128。
		Hasher hasher = Hashing.murmur3_128().newHasher();
		try (InputStream in = Files.newInputStream(filePath)) {
			byte[] bytes = new byte[8192];
			int len;
			while ((len = in.read(bytes)) >= 0) {
				hasher.putBytes(bytes, 0, len);
			}
		}
		return hasher.hash().toString();
	}
}
