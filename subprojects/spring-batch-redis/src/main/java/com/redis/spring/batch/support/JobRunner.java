package com.redis.spring.batch.support;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.job.flow.support.SimpleFlow;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.SyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.util.Assert;

public class JobRunner {

	private final JobRepository jobRepository;
	private final PlatformTransactionManager transactionManager;
	private final JobBuilderFactory jobBuilderFactory;
	private final StepBuilderFactory stepBuilderFactory;
	private final SimpleJobLauncher syncLauncher;
	private final SimpleJobLauncher asyncLauncher;

	public JobRunner(JobRepository jobRepository, PlatformTransactionManager transactionManager) {
		Assert.notNull(jobRepository, "A job repository is required");
		Assert.notNull(transactionManager, "A transaction manager is required");
		this.jobRepository = jobRepository;
		this.transactionManager = transactionManager;
		this.jobBuilderFactory = new JobBuilderFactory(jobRepository);
		this.stepBuilderFactory = new StepBuilderFactory(jobRepository, transactionManager);
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

	public JobExecution run(Job job) throws JobExecutionAlreadyRunningException, JobRestartException,
			JobInstanceAlreadyCompleteException, JobParametersInvalidException {
		return syncLauncher.run(job, new JobParameters());
	}

	public JobBuilder job(String name) {
		return jobBuilderFactory.get(name);
	}

	public StepBuilder step(String name) {
		return stepBuilderFactory.get(name);
	}

	public JobExecution runAsync(Job job) throws JobExecutionAlreadyRunningException, JobRestartException,
			JobInstanceAlreadyCompleteException, JobParametersInvalidException {
		return asyncLauncher.run(job, new JobParameters());
	}

	public FlowBuilder<SimpleFlow> flow(String name) {
		return new FlowBuilder<>(name);
	}

}
