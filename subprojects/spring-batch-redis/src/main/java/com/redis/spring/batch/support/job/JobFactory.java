package com.redis.spring.batch.support.job;

import java.time.Duration;
import java.util.concurrent.TimeoutException;

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

import lombok.Setter;

@SuppressWarnings("deprecation")
public class JobFactory {

	public static final Duration DEFAULT_IDLE_TIMEOUT = Duration.ofMillis(500);
	private static final Duration DEFAULT_STEP_COMPLETE_TIMEOUT = Duration.ofSeconds(3);
	private static final Duration DEFAULT_READER_OPEN_TIMEOUT = Duration.ofSeconds(1);
	private static final long DEFAULT_NOTIFICATION_SLEEP = 50;

	private final JobBuilderFactory jobBuilderFactory;
	private final StepBuilderFactory stepBuilderFactory;
	private final SimpleJobLauncher syncLauncher;
	private final SimpleJobLauncher asyncLauncher;
	@Setter
	private Duration stepCompleteTimeout = DEFAULT_STEP_COMPLETE_TIMEOUT;
	@Setter
	private Duration idleTimeout = DEFAULT_IDLE_TIMEOUT;
	@Setter
	private Duration readerOpenTimeout = DEFAULT_READER_OPEN_TIMEOUT;
	@Setter
	private long notificationSleep = DEFAULT_NOTIFICATION_SLEEP;
	@Setter
	private JobExecutionOptions executionOptions = JobExecutionOptions.builder().build();

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
		return jobBuilderFactory.get(name + "-job");
	}

	public <T> JobExecutionWrapper run(String name, ItemReader<? extends T> reader, ItemWriter<T> writer)
			throws JobExecutionException {
		return run(name, reader, null, writer);
	}

	public <I, O> JobExecutionWrapper run(String name, ItemReader<? extends I> reader, ItemProcessor<I, O> processor,
			ItemWriter<O> writer) throws JobExecutionException {
		return run(name, step(name, reader, processor, writer).build());
	}

	public JobExecutionWrapper run(String name, TaskletStep step) throws JobExecutionException {
		return run(job(name, step).build(), new JobParameters());
	}

	public JobExecutionWrapper run(Job job, JobParameters parameters) throws JobExecutionException {
		JobExecution execution = syncLauncher.run(job, parameters);
		return new JobExecutionWrapper(execution, executionOptions).checkForFailure();
	}

	public <I, O> JobExecutionWrapper runFlushing(String name, PollableItemReader<? extends I> reader,
			ItemWriter<O> writer) throws JobExecutionAlreadyRunningException, JobRestartException,
			JobInstanceAlreadyCompleteException, JobParametersInvalidException, InterruptedException, TimeoutException {
		return runFlushing(name, reader, null, writer);
	}

	public <I, O> JobExecutionWrapper runFlushing(String name, PollableItemReader<? extends I> reader,
			ItemProcessor<I, O> processor, ItemWriter<O> writer)
			throws JobExecutionAlreadyRunningException, JobRestartException, JobInstanceAlreadyCompleteException,
			JobParametersInvalidException, InterruptedException, TimeoutException {
		TaskletStep step = flushing(step(name, reader, processor, writer)).build();
		JobExecution jobExecution = asyncLauncher.run(job(name, step).build(), new JobParameters());
		JobExecutionWrapper execution = new JobExecutionWrapper(jobExecution, executionOptions).awaitRunning();
		awaitOpen(reader);
		return execution;
	}

	public void awaitOpen(PollableItemReader<?> reader) throws InterruptedException, TimeoutException {
		executionOptions.timer(readerOpenTimeout).await(() -> reader.getState() != null);
		if (reader.getState() == null) {
			throw new TimeoutException("Time-out while waiting for reader to become open");
		}
		notificationSleep();
	}

	public void notificationSleep() throws InterruptedException {
		Thread.sleep(notificationSleep);
	}

	public FlowBuilder<SimpleFlow> flow(String name) {
		return new FlowBuilder<>(name + "-flow");
	}

	public <I, O> FlushingStepBuilder<I, O> flushing(SimpleStepBuilder<I, O> step) {
		return new FlushingStepBuilder<>(step).idleTimeout(idleTimeout);
	}

	public <I, O> SimpleStepBuilder<I, O> step(String name, ItemReader<? extends I> reader, ItemWriter<O> writer) {
		return step(name, reader, null, writer);
	}

	public <I, O> SimpleStepBuilder<I, O> step(String name, ItemReader<? extends I> reader,
			ItemProcessor<I, O> processor, ItemWriter<O> writer) {
		return step(name).<I, O>chunk(50).reader(reader).processor(processor).writer(writer);
	}

	public StepBuilder step(String name) {
		return stepBuilderFactory.get(name + "-step");
	}

	public JobExecutionWrapper runAsync(Job job, JobParameters parameters) throws JobInstanceAlreadyCompleteException,
			JobExecutionAlreadyRunningException, JobParametersInvalidException, JobRestartException {
		return new JobExecutionWrapper(asyncLauncher.run(job, parameters), executionOptions);
	}

}
