package com.github.blovemaple.backupd.machine;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.github.blovemaple.backupd.plan.BackupConf;
import com.github.blovemaple.backupd.plan.BackupTask;

/**
 * @author blovemaple <blovemaple2010(at)gmail.com>
 *
 */
public class BackupMonitor implements Future<Void> {
	private static final Logger logger = LogManager.getLogger(BackupMonitor.class);

	private BackupConf conf;
	private Future<?> detectingFuture;
	private List<BackupTask> queuedTasks = Collections.synchronizedList(new LinkedList<>());
	private Map<BackupTask, Future<?>> startedTasks = Collections.synchronizedMap(new HashMap<>());

	private BackupDelayingQueue queue;

	private final Thread runningMonitor;
	private final Lock doneWaitingLock = new ReentrantLock();
	private final Condition taskStartCondition = doneWaitingLock.newCondition();
	private final Condition doneCondition = doneWaitingLock.newCondition();

	private boolean done = false;
	private boolean cancelled = false;

	public BackupMonitor(BackupConf conf) {
		this.conf = conf;
		runningMonitor = new Thread(new RunningMonitorTask());
		runningMonitor.setDaemon(true);
		runningMonitor.setName("running-monitor");
	}

	public BackupConf conf() {
		return conf;
	}

	public synchronized void detectingStarted(Future<?> detectingFuture) {
		this.detectingFuture = detectingFuture;
		runningMonitor.start();
	}

	public void taskQueued(BackupTask task) {
		queuedTasks.add(task);
	}

	public void taskStarted(BackupTask task, Future<?> future) {
		queuedTasks.remove(task);
		startedTasks.put(task, future);

		taskStartCondition.signalAll();
	}

	private class RunningMonitorTask implements Runnable {
		@Override
		public void run() {
			doneWaitingLock.lock();
			try {
				while (!detectingFuture.isDone()) {
					try {
						detectingFuture.get();
					} catch (ExecutionException | CancellationException e) {
					}
				}

				while (!queuedTasks.isEmpty())
					taskStartCondition.await();

				for (Future<?> backupFuture : startedTasks.values()) {
					try {
						backupFuture.get();
					} catch (ExecutionException | CancellationException e) {
					}
				}

				logger.info(() -> "Conf done: " + conf);
				done = true;
				doneCondition.signalAll();
			} catch (InterruptedException e) {
			} finally {
				doneWaitingLock.unlock();
			}
		}
	}

	@Override
	public synchronized boolean cancel(boolean mayInterruptIfRunning) {
		if (isDone())
			return false;

		if (detectingFuture != null)
			detectingFuture.cancel(true);
		queue.cancelConf(conf);
		for (Future<?> backupFuture : startedTasks.values())
			backupFuture.cancel(true);

		logger.info(() -> "Conf cancelled: " + conf);
		cancelled = true;
		return true;
	}

	@Override
	public boolean isCancelled() {
		return cancelled;
	}

	@Override
	public boolean isDone() {
		return cancelled || done;
	}

	@Override
	public Void get() throws InterruptedException, ExecutionException {
		if (cancelled)
			throw new CancellationException();

		doneWaitingLock.lockInterruptibly();
		try {
			while (!done) {
				doneCondition.await();
			}
		} finally {
			doneWaitingLock.unlock();
		}

		return null;
	}

	@Override
	public Void get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
		if (cancelled)
			throw new CancellationException();

		doneWaitingLock.lockInterruptibly();
		try {
			doneCondition.await(timeout, unit);
			if (!done)
				throw new TimeoutException();
		} finally {
			doneWaitingLock.unlock();
		}

		return null;
	}

}
