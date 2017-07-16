package com.github.blovemaple.backupd.machine;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.attribute.FileTime;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.github.blovemaple.backupd.task.BackupConf;
import com.github.blovemaple.backupd.task.BackupTask;

/**
 * 备份任务等待执行备份的队列。源文件最后修改时间后延迟指定时间后才可执行备份，延迟时间由常量{@link #DELAY_SECONDS}指定。
 * 
 * @author blovemaple <blovemaple2010(at)gmail.com>
 */
public class BackupDelayingQueue implements Closeable {
	private static final Logger logger = LogManager.getLogger(BackupDelayingQueue.class);

	/**
	 * 最后修改时间后延迟执行备份的秒数。
	 */
	public static int DELAY_SECONDS = 3;

	private final Map<BackupConf, BackupMonitor> monitors;

	// 需要延迟等待的任务
	private Map<BackupTask, Long> readyTimes = new HashMap<>();
	private PriorityQueue<BackupTask> delayingTasks = new PriorityQueue<>(Comparator.comparing(readyTimes::get));

	// 可以执行备份的任务
	private BlockingQueue<BackupTask> readyTasks = new LinkedBlockingQueue<>();

	private final Thread delayingController;

	private volatile boolean closed = false;

	public BackupDelayingQueue(Map<BackupConf, BackupMonitor> monitors) {
		this.monitors = monitors;

		delayingController = new Thread(new DelayingController());
		delayingController.setName("delaying");
		delayingController.setDaemon(true);
		delayingController.start();
	}

	/**
	 * 持续检查{@link BackupDelayingQueue#delayingTasks}中的任务，将ready的移入{@link BackupDelayingQueue#readyTasks}。
	 */
	private class DelayingController implements Runnable {

		@Override
		public void run() {
			try {
				synchronized (delayingTasks) {
					PEEK_TASK: while (true) {
						if (Thread.interrupted())
							return;

						logger.trace("Starting to peek.");

						// 取delayingTasks队列头（最近将要ready的task），若队列为空则等待
						BackupTask nextReadyTask;
						while ((nextReadyTask = delayingTasks.peek()) == null) {
							logger.trace("Peek empty, waiting.");
							delayingTasks.wait();
							logger.trace("Waked up.");
						}
						logger.trace("Peek " + nextReadyTask);

						// 取readyTime
						Long readyTime = readyTimes.get(nextReadyTask);
						if (readyTime == null)
							throw new RuntimeException("No ready time for task: " + nextReadyTask);

						// 计算需要延迟的时间
						long waitTime = readyTime - System.currentTimeMillis();
						while (waitTime > 0) {
							logger.trace("Delaying time " + waitTime + ", waiting.");
							// 在delayingTasks上等待延迟
							delayingTasks.wait(waitTime);

							BackupTask nextReadyTaskNow = delayingTasks.peek();
							if (nextReadyTaskNow != nextReadyTask) {
								// 等待延迟期间delayingTasks队列头已改变，重新peek
								logger.trace("Queue head is changed, repeek.");
								continue PEEK_TASK;
							}

							waitTime = readyTime - System.currentTimeMillis();
						}

						// 将task从delayingTasks移到readyTasks
						BackupTask readyTask = delayingTasks.poll();
						if (readyTask != nextReadyTask)
							throw new RuntimeException("delayingTasks is modified concurrently! nextReadyTask="
									+ nextReadyTask + ", readyTask=" + readyTask);
						readyTimes.remove(readyTask);
						readyTasks.add(readyTask);
						logger.debug(() -> "Move into delayingTasks: " + readyTask);
					}

				}
			} catch (InterruptedException e) {
				return;
			}

		}

	}

	/**
	 * 提交一个备份任务。
	 * 
	 * @param task
	 * @throws InterruptedException
	 * @throws IOException
	 */
	public void submit(BackupTask task) throws InterruptedException, IOException {
		if (Thread.interrupted())
			throw new InterruptedException();
		if (closed)
			throw new IllegalStateException("Already closed.");

		BackupMonitor monitor = monitors.get(task.conf());
		if (monitor != null)
			monitor.taskQueued(task);

		synchronized (delayingTasks) {
			// 先从队列中删除该任务（如果已经有的话）
			if (readyTimes.remove(task) != null)
				delayingTasks.remove(task);
			else
				readyTasks.remove(task);

			// 根据readyTime决定放到delayingTasks还是readyTasks
			long readyTime = getReadyTime(task);
			if (readyTime <= System.currentTimeMillis()) {
				// ready
				readyTasks.put(task);
				logger.debug(() -> "Submit into readyTasks: " + task);
			} else {
				// not ready
				readyTimes.put(task, readyTime);
				delayingTasks.add(task);
				delayingTasks.notify(); // 唤醒DelayingControllerTask
				logger.debug(() -> "Submit into delayingTasks: " + task);
			}
		}
	}

	private long getReadyTime(BackupTask task) throws IOException {
		try {
			FileTime modifiedTime = Files.getLastModifiedTime(task.fromFullPath());
			return modifiedTime.toMillis() + DELAY_SECONDS * 1000;
		} catch (NoSuchFileException e) {
			// 文件被删除，不delay
			return System.currentTimeMillis();
		}
	}

	/**
	 * 取消队列中指定配置的所有备份任务。
	 * 
	 * @param conf
	 *            配置
	 */
	public void cancelConf(BackupConf conf) {
		synchronized (delayingTasks) {
			Iterator<BackupTask> itr = delayingTasks.iterator();
			while (itr.hasNext()) {
				BackupTask task = itr.next();
				if (task.conf() == conf)
					itr.remove();
			}
		}

		Iterator<BackupTask> itr = readyTasks.iterator();
		while (itr.hasNext()) {
			BackupTask task = itr.next();
			if (task.conf() == conf)
				itr.remove();
		}
	}

	/**
	 * 取出一个备份任务，如无任务可取则等待直到取出或超时。
	 * 
	 * @param waitingSeconds
	 *            等待秒数
	 * @return 备份任务，超时返回null
	 */
	public BackupTask fetch(int waitingSeconds) throws InterruptedException {
		if (Thread.interrupted())
			throw new InterruptedException();
		if (closed)
			throw new IllegalStateException("Already closed.");

		BackupTask task = readyTasks.poll(waitingSeconds, TimeUnit.SECONDS);
		logger.debug(() -> "Fetch: " + task);
		return task;
	}

	@Override
	public void close() {
		closed = true;
		delayingController.interrupt();
	}
}
