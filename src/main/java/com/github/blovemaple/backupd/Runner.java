package com.github.blovemaple.backupd;

import java.awt.AWTException;
import java.awt.Image;
import java.awt.SystemTray;
import java.awt.Toolkit;
import java.awt.TrayIcon;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import com.github.blovemaple.backupd.machine.BackupMachine;
import com.github.blovemaple.backupd.machine.BackupMonitor;
import com.github.blovemaple.backupd.task.BackupConf;

/**
 * 主类。
 * 
 * @author blovemaple <blovemaple2010(at)gmail.com>
 */
public class Runner {
	private static Arguments arguments = new Arguments();
	private static JCommander commander;
	static {
		commander = JCommander.newBuilder().addObject(arguments).build();
		commander.setProgramName("backupd");
	}

	private static List<BackupConf> confs;

	private static BackupMachine machine;
	private static List<BackupMonitor> monitors;

	public static void main(String[] args) throws InterruptedException {
		parseArgs(args);
		parseConfig();
		execute();

		// if (SystemTray.isSupported()) {
		// addTray();
		// }

	}

	private static void parseArgs(String[] args) {
		try {
			commander.parse(args);
		} catch (ParameterException e) {
			showUsageAndExit(e.getLocalizedMessage());
		}

		if (arguments.isHelp())
			showUsageAndExit();
	}

	private static void parseConfig() {
		try {
			confs = ConfGenerator.fromConfLines(Files.lines(arguments.getConfigFilePath()));
		} catch (NoSuchFileException e) {
			showUsageAndExit("No such config file: " + arguments.getConfigFilePath());
		} catch (IOException e) {
			showUsageAndExit(
					"Cannot open config file: " + arguments.getConfigFilePath() + ": " + e.getLocalizedMessage());
		}
	}

	private static void execute() {
		confs.forEach(conf -> {
			try {
				conf.validate();
			} catch (Exception e) {
				showUsageAndExit("Illegal config " + conf.getName() + ": " + e.getLocalizedMessage());
			}
		});

		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				if (monitors != null) {
					monitors.forEach(m -> m.cancel(true));
				}
			}
		});

		machine = new BackupMachine();
		monitors = confs.stream().map(machine::execute).collect(Collectors.toList());
	}

	private static void showUsageAndExit(String... errorMessage) {
		StringBuilder outStr = new StringBuilder();
		Arrays.stream(errorMessage).forEach(msg -> outStr.append(msg).append(System.lineSeparator()));
		commander.usage(outStr);

		if (errorMessage.length > 0)
			System.err.println(outStr);
		else
			System.out.println(outStr);
		System.exit(1);
	}

	@SuppressWarnings("unused")
	private static void addTray() {
		SystemTray tray = SystemTray.getSystemTray();
		Image image = Toolkit.getDefaultToolkit().getImage(Runner.class.getResource("/backupd2-ing.png"));
		TrayIcon trayIcon = new TrayIcon(image, "Tray Demo");
		trayIcon.setImageAutoSize(true);
		try {
			tray.add(trayIcon);
		} catch (AWTException e) {
			e.printStackTrace();
		}

		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				tray.remove(trayIcon);
			}
		});
	}

}
