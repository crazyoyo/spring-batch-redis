package com.redis.spring.batch.support;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.Callable;

import org.awaitility.Awaitility;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionException;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.job.flow.support.SimpleFlow;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemStreamSupport;
import org.springframework.batch.item.ItemWriter;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.SyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.util.Assert;

public class JobRunner {

	public static final Duration DEFAULT_RUNNING_TIMEOUT = Duration.ofSeconds(5);
	public static final Duration DEFAULT_TERMINATION_TIMEOUT = Duration.ofSeconds(5);

	private Duration runningTimeout = DEFAULT_RUNNING_TIMEOUT;
	private Duration terminationTimeout = DEFAULT_TERMINATION_TIMEOUT;
	private final JobRepository jobRepository;
	private final PlatformTransactionManager transactionManager;
	private final SimpleJobLauncher jobLauncher;
	private final SimpleJobLauncher asyncJobLauncher;

	public JobRunner(JobRepository jobRepository, PlatformTransactionManager transactionManager) {
		Assert.notNull(jobRepository, "A job repository is required");
		Assert.notNull(transactionManager, "A transaction manager is required");
		this.jobRepository = jobRepository;
		this.transactionManager = transactionManager;
		this.jobLauncher = launcher(new SyncTaskExecutor());
		this.asyncJobLauncher = launcher(new SimpleAsyncTaskExecutor());
	}

	public void setRunningTimeout(Duration runningTimeout) {
		Utils.assertPositive(runningTimeout, "Running timeout");
		this.runningTimeout = runningTimeout;
	}

	public void setTerminationTimeout(Duration terminationTimeout) {
		Utils.assertPositive(terminationTimeout, "Termination timeout");
		this.terminationTimeout = terminationTimeout;
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

	public JobExecution run(Job job) throws JobExecutionException {
		return awaitTermination(jobLauncher.run(job, new JobParameters()));
	}

	public static JobExecution awaitTermination(JobExecution execution, Duration timeout) throws JobExecutionException {
		if (execution != null) {
			Awaitility.await().timeout(timeout).until(() -> isTerminated(execution));
		}
		return checkUnsuccessful(execution);
	}

	public static boolean isTerminated(JobExecution execution) {
		return execution.getStatus() == BatchStatus.COMPLETED
				|| execution.getStatus().isGreaterThan(BatchStatus.STOPPED);
	}

	public JobExecution awaitTermination(JobExecution execution) throws JobExecutionException {
		return awaitTermination(execution, terminationTimeout);
	}

	public void awaitRunning(Callable<Boolean> callable) {
		Awaitility.await().timeout(runningTimeout).until(callable);
	}

	public JobExecution awaitRunning(JobExecution execution) throws JobExecutionException {
		return awaitRunning(execution, runningTimeout);
	}

	public static JobExecution awaitRunning(JobExecution execution, Duration timeout) throws JobExecutionException {
		if (execution != null) {
			Awaitility.await().timeout(timeout).until(() -> execution.getStatus() != BatchStatus.STARTING);
		}
		return checkUnsuccessful(execution);
	}

	public JobExecution runAsync(Job job) throws JobExecutionException {
		return awaitRunning(asyncJobLauncher.run(job, new JobParameters()));
	}

	private static JobExecution checkUnsuccessful(JobExecution execution) throws JobExecutionException {
		if (execution == null) {
			throw new JobExecutionException("Job execution is null");
		}
		if (execution.getStatus().isUnsuccessful()) {
			String message = String.format("Status of job '%s': %s", execution.getJobInstance().getJobName(),
					execution.getStatus());
			List<Throwable> exceptions = execution.getAllFailureExceptions();
			if (exceptions.isEmpty()) {
				throw new JobExecutionException(message);
			}
			throw new JobExecutionException(message, exceptions.get(0));
		}
		return execution;
	}

	public <I, O> JobExecution run(String name, int chunkSize, ItemReader<I> reader, ItemWriter<O> writer)
			throws JobExecutionException {
		return run(job(name, chunkSize, reader, writer));
	}

	public <I, O> Job job(String name, int chunkSize, ItemReader<I> reader, ItemWriter<O> writer) {
		return job(name, step(name, chunkSize, reader, writer));
	}

	public Job job(String name, SimpleStepBuilder<?, ?> step) {
		return job(name).start(step.build()).build();
	}

	public <I, O> SimpleStepBuilder<I, O> step(String name, int chunkSize, ItemReader<I> reader, ItemWriter<O> writer) {
		if (reader instanceof ItemStreamSupport) {
			((ItemStreamSupport) reader).setName(name + "-reader");
		}
		if (writer instanceof ItemStreamSupport) {
			((ItemStreamSupport) writer).setName(name + "-writer");
		}
		return step(name).<I, O>chunk(chunkSize).reader(reader).writer(writer);
	}

	public <I, O> JobExecution runAsync(String name, int chunkSize, ItemReader<I> reader, ItemWriter<O> writer)
			throws JobExecutionException {
		return runAsync(job(name, chunkSize, reader, writer));
	}

	public static JobRunner inMemory() throws Exception {
		@SuppressWarnings("deprecation")
		org.springframework.batch.core.repository.support.MapJobRepositoryFactoryBean bean = new org.springframework.batch.core.repository.support.MapJobRepositoryFactoryBean();
		bean.afterPropertiesSet();
		return new JobRunner(bean.getObject(), bean.getTransactionManager());
	}

}
