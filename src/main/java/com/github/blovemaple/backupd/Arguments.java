package com.github.blovemaple.backupd;

import java.nio.file.Path;

import com.beust.jcommander.Parameter;

/**
 * @author blovemaple <blovemaple2010(at)gmail.com>
 */
public class Arguments {
	@Parameter(names = { "-c", "--config" }, description = "Config file path", required = true)
	private Path configFilePath;

	@Parameter(names = "--help", description = "Desplay help", help = true)
	private boolean help;

	public Path getConfigFilePath() {
		return configFilePath;
	}

	public void setConfigFilePath(Path configFilePath) {
		this.configFilePath = configFilePath;
	}

	public boolean isHelp() {
		return help;
	}

	public void setHelp(boolean help) {
		this.help = help;
	}

}
