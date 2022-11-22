package com.redis.spring.batch.common;

import java.util.concurrent.atomic.AtomicInteger;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionException;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemStreamSupport;
import org.springframework.util.ClassUtils;

public abstract class JobExecutionItemStream extends ItemStreamSupport {

	protected final JobRunner jobRunner;
	private final AtomicInteger runningThreads = new AtomicInteger();

	protected JobExecution jobExecution;
	protected String name;

	protected JobExecutionItemStream(JobRunner jobRunner) {
		setName(ClassUtils.getShortName(getClass()));
		this.jobRunner = jobRunner;
	}

	@Override
	public void setName(String name) {
		this.name = name;
		super.setName(name);
	}

	@Override
	public void open(ExecutionContext executionContext) throws ItemStreamException {
		synchronized (runningThreads) {
			if (jobExecution == null) {
				doOpen();
			}
			runningThreads.incrementAndGet();
			super.open(executionContext);
		}
	}

	protected void doOpen() {
		try {
			jobExecution = jobRunner.runAsync(job());
		} catch (JobExecutionException e) {
			throw new ItemStreamException(String.format("Could not run job %s", name), e);
		}
	}

	protected abstract Job job();

	@Override
	public void close() {
		super.close();
		if (runningThreads.decrementAndGet() > 0) {
			return;
		}
		synchronized (runningThreads) {
			doClose();
		}
	}

	protected void doClose() {
		jobRunner.awaitTermination(jobExecution);
		jobExecution = null;
	}

	public boolean isOpen() {
		return jobExecution != null;
	}

}
