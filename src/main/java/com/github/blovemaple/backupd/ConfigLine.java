package com.github.blovemaple.backupd;

import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;

import com.google.common.base.Strings;

/**
 * @author blovemaple <blovemaple2010(at)gmail.com>
 */
public class ConfigLine {
	private static final String COMMENT_SIGN = "#";
	private static final String REMOTE_PATH_SIGN = "remote";

	public static enum ConfigLineType {
		NAME("backup"), FROM("from"), TO("to"), FILTER("only");
		private final String literal;

		private ConfigLineType(String literal) {
			this.literal = literal;
		}

		public String getLiteral() {
			return literal;
		}
	}

	public static ConfigLine parse(String line) {
		int commentIndex = line.indexOf(COMMENT_SIGN);
		String lineWithoutComment = commentIndex >= 0 ? line.substring(0, commentIndex) : line;

		String[] typeAndContent = lineWithoutComment.trim().split("\\s+", 2);
		ConfigLineType type = Arrays.stream(ConfigLineType.values())
				.filter(aType -> aType.getLiteral().equalsIgnoreCase(typeAndContent[0])).findFirst()
				.orElseThrow(() -> new IllegalArgumentException("Invalid config line: " + line));
		String content = typeAndContent.length > 1 ? typeAndContent[1] : null;
		return new ConfigLine(line, type, content);
	}

	private String line;
	private ConfigLineType type;
	private String content;

	private ConfigLine(String line, ConfigLineType type, String content) {
		this.line = line;
		this.type = type;
		this.content = content;
	}

	public String getLine() {
		return line;
	}

	public void setLine(String line) {
		this.line = line;
	}

	public ConfigLineType getType() {
		return type;
	}

	public void setType(ConfigLineType type) {
		this.type = type;
	}

	public String getContent() {
		return content;
	}

	public Path getPath() {
		if (Strings.isNullOrEmpty(content)) {
			throw new IllegalStateException("Cannot parse Path from empty content of config line: " + line);
		}
		String[] signAndPath = content.trim().split("\\s+", 2);
		if (signAndPath.length > 1 && signAndPath[0].equals(REMOTE_PATH_SIGN))
			return Paths.get(URI.create(signAndPath[1]));
		else
			return Paths.get(content);
	}

	public void setContent(String content) {
		this.content = content;
	}

}
