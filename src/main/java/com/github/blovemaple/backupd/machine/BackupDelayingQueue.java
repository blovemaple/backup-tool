package com.github.blovemaple.backupd.machine;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.attribute.FileTime;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import com.github.blovemaple.backupd.plan.BackupTask;

/**
 * 检测任务提交的文件等待执行备份的队列。每个计划有一个。 TODO 总共用一个
 * 
 * @author blovemaple <blovemaple2010(at)gmail.com>
 */
public class BackupDelayingQueue implements Closeable {
	// 延迟秒数
	private static final int DELAY_SECONDS = 3;

	// 需要延迟等待的任务
	private Map<BackupTask, Long> readyTimes = new HashMap<>();
	private PriorityQueue<BackupTask> delayingTasks = new PriorityQueue<>(Comparator.comparing(readyTimes::get));

	// 可以执行备份的任务
	private BlockingQueue<BackupTask> readyTasks = new LinkedBlockingQueue<>();

	private final Thread delayingController;

	private volatile boolean entranceClosed = false;
	private volatile boolean closed = false;

	public BackupDelayingQueue() {
		delayingController = new Thread(new DelayingController());
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

						// 取delayingTasks队列头（最近将要ready的task），若队列为空则等待
						BackupTask nextReadyTask;
						while ((nextReadyTask = delayingTasks.peek()) == null) {
							delayingTasks.wait();
						}

						// 取readyTime
						Long readyTime = readyTimes.get(nextReadyTask);
						if (readyTime == null)
							throw new RuntimeException("No ready time for task: " + nextReadyTask);

						// 计算需要延迟的时间
						long waitTime = readyTime - System.currentTimeMillis();
						while (waitTime > 0) {
							// 在delayingTasks上等待延迟
							delayingTasks.wait(waitTime);

							BackupTask nextReadyTaskNow = delayingTasks.peek();
							if (nextReadyTaskNow != nextReadyTask)
								// 等待延迟期间delayingTasks队列头已改变，重新peek
								continue PEEK_TASK;

							waitTime = readyTime - System.currentTimeMillis();
						}

						// 将task从delayingTasks移到readyTasks
						BackupTask readyTask = delayingTasks.poll();
						if (readyTask != nextReadyTask)
							throw new RuntimeException("delayingTasks is modified concurrently! nextReadyTask="
									+ nextReadyTask + ", readyTask=" + readyTask);
						readyTimes.remove(readyTask);
						readyTasks.add(readyTask);
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
	 * @throws ClosedQueueException
	 * @throws IOException
	 */
	public void submit(BackupTask task) throws InterruptedException, ClosedQueueException, IOException {
		if (Thread.interrupted())
			throw new InterruptedException();
		if (entranceClosed || closed)
			throw new ClosedQueueException();

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
			} else {
				// not ready
				readyTimes.put(task, readyTime);
				delayingTasks.add(task);
				delayingTasks.notify(); // 唤醒DelayingControllerTask
			}
		}
	}

	private long getReadyTime(BackupTask task) throws IOException {
		FileTime modifiedTime = Files.getLastModifiedTime(task.fromFullPath());
		return modifiedTime.toMillis() + DELAY_SECONDS;
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
			throw new ClosedQueueException();

		return readyTasks.poll(waitingSeconds, TimeUnit.SECONDS);
	}

	/**
	 * 停止提交。调用后再提交会抛出异常{@link ClosedQueueException}。
	 */
	public void closeEntrance() {
		entranceClosed = true;
	}

	@Override
	public void close() {
		closeEntrance();
		closed = true;
		delayingController.interrupt();
	}
}
