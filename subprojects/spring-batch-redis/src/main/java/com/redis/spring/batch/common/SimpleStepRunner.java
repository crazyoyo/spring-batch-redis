package com.redis.spring.batch.common;

import java.time.Duration;

import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionException;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemStreamSupport;
import org.springframework.batch.item.ItemWriter;
import org.springframework.util.ClassUtils;

public class SimpleStepRunner<I, O> extends ItemStreamSupport {

	public static final Duration DEFAULT_RUNNING_TIMEOUT = Duration.ofSeconds(5);
	public static final Duration DEFAULT_FLUSHING_INTERVAL = Duration.ofMillis(50);

	private final JobRunner jobRunner;
	private final ItemReader<I> reader;
	private final ItemProcessor<I, O> processor;
	private final ItemWriter<O> writer;
	private final StepOptions options;

	private String name;
	private JobExecution jobExecution;

	public SimpleStepRunner(JobRunner jobRunner, ItemReader<I> reader, ItemProcessor<I, O> processor,
			ItemWriter<O> writer, StepOptions stepOptions) {
		setName(ClassUtils.getShortName(getClass()));
		this.jobRunner = jobRunner;
		this.reader = reader;
		this.processor = processor;
		this.writer = writer;
		this.options = stepOptions;
	}

	@Override
	public void setName(String name) {
		super.setName(name);
		this.name = name;
	}

	@Override
	public void open(ExecutionContext executionContext) {
		if (jobExecution == null) {
			SimpleStepBuilder<I, O> step = jobRunner.step(name, reader, processor, writer, options);
			Job job = jobRunner.job(name).start(step.build()).build();
			try {
				jobExecution = jobRunner.runAsync(job);
			} catch (JobExecutionException e) {
				throw new ItemStreamException("Job execution failed", e);
			}
			if (jobExecution.getStatus().isUnsuccessful()) {
				throw new ItemStreamException("Job execution unsuccessful");
			}
		}
		super.open(executionContext);
	}

	public boolean isRunning() {
		return jobExecution != null && jobExecution.isRunning();
	}

	public boolean isJobFailed() {
		return jobExecution != null && jobExecution.getStatus().isUnsuccessful();
	}

	@Override
	public void close() {
		super.close();
		if (jobExecution != null) {
			if (jobExecution.isRunning()) {
				for (StepExecution stepExecution : jobExecution.getStepExecutions()) {
					stepExecution.setTerminateOnly();
				}
				jobExecution.setStatus(BatchStatus.STOPPING);
				jobRunner.awaitNotRunning(jobExecution);
			}
			jobExecution = null;
		}
	}

}
