package com.redis.spring.batch.support.job;

import java.time.Duration;
import java.util.concurrent.TimeoutException;

import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionException;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.job.builder.SimpleJobBuilder;
import org.springframework.batch.core.job.flow.support.SimpleFlow;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.batch.core.repository.support.MapJobRepositoryFactoryBean;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.SyncTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

import com.redis.spring.batch.support.FlushingStepBuilder;
import com.redis.spring.batch.support.PollableItemReader;
import com.redis.spring.batch.support.Timer;

import lombok.Builder;
import lombok.Builder.Default;
import lombok.Data;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@SuppressWarnings("deprecation")
public class JobFactory {

	private final JobBuilderFactory jobBuilderFactory;
	private final StepBuilderFactory stepBuilderFactory;
	private final SimpleJobLauncher syncLauncher;
	private final SimpleJobLauncher asyncLauncher;
	@Setter
	private Options options = Options.builder().build();

	public JobFactory(JobRepository jobRepository, PlatformTransactionManager transactionManager) throws Exception {
		jobBuilderFactory = new JobBuilderFactory(jobRepository);
		stepBuilderFactory = new StepBuilderFactory(jobRepository, transactionManager);
		syncLauncher = new SimpleJobLauncher();
		syncLauncher.setJobRepository(jobRepository);
		syncLauncher.setTaskExecutor(new SyncTaskExecutor());
		syncLauncher.afterPropertiesSet();
		asyncLauncher = new SimpleJobLauncher();
		asyncLauncher.setJobRepository(jobRepository);
		asyncLauncher.setTaskExecutor(new SimpleAsyncTaskExecutor());
		asyncLauncher.afterPropertiesSet();
	}

	public static JobFactory inMemory() throws Exception {
		MapJobRepositoryFactoryBean bean = new MapJobRepositoryFactoryBean();
		return new JobFactory(bean.getObject(), bean.getTransactionManager());
	}

	public SimpleJobBuilder job(String name, TaskletStep step) {
		return job(name).start(step);
	}

	public JobBuilder job(String name) {
		log.info("Creating job '{}'", name);
		return jobBuilderFactory.get(name);
	}

	public FlowBuilder<SimpleFlow> flow(String name) {
		return new FlowBuilder<>(name);
	}

	public StepBuilder step(String name) {
		return stepBuilderFactory.get(name);
	}

	public <T> JobExecution run(String name, int chunkSize, ItemReader<? extends T> reader, ItemWriter<T> writer)
			throws JobExecutionException {
		return run(name, chunkSize, reader, null, writer);
	}

	public <I, O> JobExecution run(String name, int chunkSize, ItemReader<? extends I> reader,
			ItemProcessor<I, O> processor, ItemWriter<O> writer) throws JobExecutionException {
		return run(name, step(name, chunkSize, reader, processor, writer).build());
	}

	public JobExecution run(String name, TaskletStep step) throws JobExecutionException {
		return run(job(name, step).build(), new JobParameters());
	}

	public JobExecution run(Job job, JobParameters parameters) throws JobExecutionException {
		JobExecution execution = syncLauncher.run(job, parameters);
		checkForFailure(execution);
		return execution;
	}

	public void awaitRunning(JobExecution execution) throws InterruptedException, TimeoutException {
		Timer.timeout(options.getRunningTimeout()).await(() -> execution.isRunning());
		if (!execution.isRunning()) {
			throw new TimeoutException("Timeout while waiting for job to run");
		}
	}

	public void awaitTermination(JobExecution execution)
			throws InterruptedException, TimeoutException, JobExecutionException {
		Timer.timeout(options.getTerminationTimeout()).await(() -> !execution.isRunning());
		checkForFailure(execution);
	}

	public void checkForFailure(JobExecution execution) throws JobExecutionException {
		if (!execution.getExitStatus().getExitCode().equals(ExitStatus.COMPLETED.getExitCode())) {
			throw new JobExecutionException("Job failed: " + execution.getExitStatus());
		}
	}

	public <T> JobExecution runFlushing(String name, int chunkSize, PollableItemReader<? extends T> reader,
			ItemWriter<T> writer) throws JobExecutionAlreadyRunningException, JobRestartException,
			JobInstanceAlreadyCompleteException, JobParametersInvalidException, InterruptedException, TimeoutException {
		return runFlushing(name, chunkSize, reader, null, writer);
	}

	public <I, O> JobExecution runFlushing(String name, int chunkSize, PollableItemReader<? extends I> reader,
			ItemProcessor<I, O> processor, ItemWriter<O> writer)
			throws JobExecutionAlreadyRunningException, JobRestartException, JobInstanceAlreadyCompleteException,
			JobParametersInvalidException, InterruptedException, TimeoutException {
		TaskletStep step = flushing(step(name, chunkSize, reader, processor, writer)).build();
		JobExecution jobExecution = asyncLauncher.run(job(name, step).build(), new JobParameters());
		awaitRunning(jobExecution);
		awaitOpen(reader);
		return jobExecution;
	}

	public void awaitOpen(PollableItemReader<?> reader) throws InterruptedException, TimeoutException {
		Timer.timeout(options.getReaderOpenTimeout()).await(() -> reader.getState() != null);
		if (reader.getState() == null) {
			throw new TimeoutException("Time-out while waiting for reader to become open");
		}
		notificationSleep();
	}

	public void notificationSleep() throws InterruptedException {
		Thread.sleep(options.getNotificationSleep());
	}

	public <I, O> FlushingStepBuilder<I, O> flushing(SimpleStepBuilder<I, O> step) {
		return new FlushingStepBuilder<>(step).idleTimeout(options.getIdleTimeout());
	}

	public <T> SimpleStepBuilder<T, T> step(String name, int chunkSize, ItemReader<? extends T> reader,
			ItemWriter<T> writer) {
		return step(name, chunkSize, reader, null, writer);
	}

	public <I, O> SimpleStepBuilder<I, O> step(String name, int chunkSize, ItemReader<? extends I> reader,
			ItemProcessor<I, O> processor, ItemWriter<O> writer) {
		return step(name).<I, O>chunk(chunkSize).reader(reader).processor(processor).writer(writer);
	}

	public JobExecution runAsync(Job job, JobParameters parameters) throws JobInstanceAlreadyCompleteException,
			JobExecutionAlreadyRunningException, JobParametersInvalidException, JobRestartException {
		return asyncLauncher.run(job, parameters);
	}

	@Data
	@Builder
	public static class Options {

		public static final Duration DEFAULT_IDLE_TIMEOUT = Duration.ofMillis(1000);
		public static final Duration DEFAULT_READER_OPEN_TIMEOUT = Duration.ofSeconds(1);
		public static final long DEFAULT_NOTIFICATION_SLEEP = 50;
		public static final Duration DEFAULT_TERMINATION_TIMEOUT = Duration.ofSeconds(30);
		public static final Duration DEFAULT_RUNNING_TIMEOUT = Duration.ofSeconds(3);
		public static final Duration DEFAULT_SLEEP = Duration.ofMillis(1);
		@Default
		private Duration idleTimeout = DEFAULT_IDLE_TIMEOUT;
		@Default
		private Duration readerOpenTimeout = DEFAULT_READER_OPEN_TIMEOUT;
		@Default
		private long notificationSleep = DEFAULT_NOTIFICATION_SLEEP;
		@Default
		private Duration runningTimeout = DEFAULT_RUNNING_TIMEOUT;
		@Default
		private Duration terminationTimeout = DEFAULT_TERMINATION_TIMEOUT;
		@Default
		private Duration sleep = DEFAULT_SLEEP;

	}

}
