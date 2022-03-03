package com.redis.spring.batch.support;

import java.time.Duration;

import org.awaitility.Awaitility;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionException;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.job.flow.support.SimpleFlow;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.SyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.util.Assert;

import com.redis.spring.batch.step.FlushingStepBuilder;

public class JobRunner {

	private final JobRepository jobRepository;
	private final PlatformTransactionManager transactionManager;
	private final SimpleJobLauncher syncLauncher;
	private final SimpleJobLauncher asyncLauncher;

	public JobRunner(JobRepository jobRepository, PlatformTransactionManager transactionManager) {
		Assert.notNull(jobRepository, "A job repository is required");
		Assert.notNull(transactionManager, "A transaction manager is required");
		this.jobRepository = jobRepository;
		this.transactionManager = transactionManager;
		this.syncLauncher = launcher(new SyncTaskExecutor());
		this.asyncLauncher = launcher(new SimpleAsyncTaskExecutor());
	}

	public JobRepository getJobRepository() {
		return jobRepository;
	}

	public PlatformTransactionManager getTransactionManager() {
		return transactionManager;
	}

	private SimpleJobLauncher launcher(TaskExecutor taskExecutor) {
		SimpleJobLauncher launcher = new SimpleJobLauncher();
		launcher.setJobRepository(jobRepository);
		launcher.setTaskExecutor(taskExecutor);
		return launcher;
	}

	public JobBuilder job(String name) {
		return new JobBuilder(name).repository(jobRepository);
	}

	public FlowBuilder<SimpleFlow> flow(String name) {
		return new FlowBuilder<>(name);
	}

	public StepBuilder step(String name) {
		return new StepBuilder(name).repository(jobRepository).transactionManager(transactionManager);
	}

	public FlushingStepBuilder flushingStep(String name) {
		return new FlushingStepBuilder(name).repository(jobRepository).transactionManager(transactionManager);
	}

	public JobExecution run(Job job) throws JobExecutionException {
		return awaitTermination(syncLauncher.run(job, new JobParameters()));
	}

	public static JobExecution awaitTermination(JobExecution execution) throws JobExecutionException {
		Awaitility.await().timeout(Duration.ofMinutes(1))
				.until(() -> !execution.isRunning() || execution.getStatus().isUnsuccessful());
		return checkUnsuccessful(execution);
	}

	public JobExecution runAsync(Job job) throws JobExecutionException {
		return asyncLauncher.run(job, new JobParameters());
	}

	public static JobExecution awaitRunning(JobExecution execution) throws JobExecutionException {
		Awaitility.await().until(() -> execution.isRunning() || execution.getStatus().isUnsuccessful());
		return checkUnsuccessful(execution);
	}

	private static JobExecution checkUnsuccessful(JobExecution execution) throws JobExecutionException {
		if (execution.getStatus().isUnsuccessful()) {
			throw new JobExecutionException(String.format("Status of job '%s': %s",
					execution.getJobInstance().getJobName(), execution.getStatus()));
		}
		return execution;
	}

}
