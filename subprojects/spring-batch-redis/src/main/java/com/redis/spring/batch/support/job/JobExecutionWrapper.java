package com.redis.spring.batch.support.job;

import java.time.Duration;
import java.util.concurrent.TimeoutException;

import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionException;

public class JobExecutionWrapper {

	private final JobExecution jobExecution;
	private final JobExecutionOptions options;

	public JobExecutionWrapper(JobExecution jobExecution, JobExecutionOptions options) {
		this.jobExecution = jobExecution;
		this.options = options;
	}
	
	public JobExecution getJobExecution() {
		return jobExecution;
	}

	public boolean isRunning() {
		return jobExecution.isRunning();
	}

	public JobExecutionWrapper awaitRunning() throws InterruptedException, TimeoutException {
		return awaitRunning(options.getRunningTimeout());
	}

	public JobExecutionWrapper awaitRunning(Duration timeout) throws InterruptedException, TimeoutException {
		options.timer(timeout).await(() -> jobExecution.isRunning());
		if (!jobExecution.isRunning()) {
			throw new TimeoutException("Timeout while waiting for job to run");
		}
		return this;
	}
	
	public JobExecutionWrapper awaitTermination() throws InterruptedException, TimeoutException, JobExecutionException {
		return awaitTermination(options.getTerminationTimeout());
	}

	public JobExecutionWrapper awaitTermination(Duration timeout) throws InterruptedException, TimeoutException, JobExecutionException {
		options.timer(timeout).await(() -> !jobExecution.isRunning());
		return checkForFailure();
	}

	public JobExecutionWrapper checkForFailure() throws JobExecutionException {
		if (!jobExecution.getExitStatus().getExitCode().equals(ExitStatus.COMPLETED.getExitCode())) {
			throw new JobExecutionException("Job status: " + jobExecution.getExitStatus());
		}
		return this;
	}
}