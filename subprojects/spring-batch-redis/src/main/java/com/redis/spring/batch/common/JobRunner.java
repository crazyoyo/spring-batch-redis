package com.redis.spring.batch.common;

import java.time.Duration;

import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.FaultTolerantStepBuilder;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.batch.item.support.SynchronizedItemStreamReader;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.SyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

import com.redis.spring.batch.reader.PollableItemReader;
import com.redis.spring.batch.step.FlushingSimpleStepBuilder;

public class JobRunner {

	public static final Duration DEFAULT_RUNNING_TIMEOUT = Duration.ofSeconds(5);
	public static final Duration DEFAULT_TERMINATION_TIMEOUT = Duration.ofSeconds(5);

	private final JobRepository jobRepository;
	private final PlatformTransactionManager transactionManager;
	private final SimpleJobLauncher jobLauncher;
	private final SimpleJobLauncher asyncJobLauncher;

	public JobRunner(JobRepository jobRepository, PlatformTransactionManager transactionManager) {
		this.jobRepository = jobRepository;
		this.transactionManager = transactionManager;
		this.jobLauncher = launcher(new SyncTaskExecutor());
		this.asyncJobLauncher = launcher(new SimpleAsyncTaskExecutor());
	}

	public static JobRunner inMemory() {
		@SuppressWarnings("deprecation")
		org.springframework.batch.core.repository.support.MapJobRepositoryFactoryBean bean = new org.springframework.batch.core.repository.support.MapJobRepositoryFactoryBean();
		try {
			bean.afterPropertiesSet();
			return new JobRunner(bean.getObject(), bean.getTransactionManager());
		} catch (Exception e) {
			throw new RuntimeException("Could not initialize in-memory job runner", e);
		}
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

	public StepBuilder step(String name) {
		return new StepBuilder(name).repository(jobRepository).transactionManager(transactionManager);
	}

	public static <T> ItemReader<T> synchronize(ItemReader<T> reader) {
		if (reader instanceof PollableItemReader) {
			return new SynchronizedPollableItemReader<>((PollableItemReader<T>) reader);
		}
		if (reader instanceof ItemStreamReader) {
			return synchronizedItemStreamReader((ItemStreamReader<T>) reader);
		}
		return reader;
	}

	private static <T> ItemReader<T> synchronizedItemStreamReader(ItemStreamReader<T> reader) {
		SynchronizedItemStreamReader<T> synchronizedReader = new SynchronizedItemStreamReader<>();
		synchronizedReader.setDelegate(reader);
		return synchronizedReader;
	}

	public static <I, O> FlushingSimpleStepBuilder<I, O> flushing(SimpleStepBuilder<I, O> step,
			FlushingOptions options) {
		FlushingSimpleStepBuilder<I, O> flushingStep = new FlushingSimpleStepBuilder<>(step);
		flushingStep.flushingInterval(options.getFlushingInterval());
		flushingStep.idleTimeout(options.getIdleTimeout());
		return flushingStep;
	}

	public static <I, O> FaultTolerantStepBuilder<I, O> faultTolerant(SimpleStepBuilder<I, O> step,
			FaultToleranceOptions options) {
		FaultTolerantStepBuilder<I, O> ftStep = step.faultTolerant();
		ftStep.skipLimit(options.getSkipLimit());
		options.getSkip().forEach(ftStep::skip);
		options.getNoSkip().forEach(ftStep::noSkip);
		options.getSkipPolicy().ifPresent(ftStep::skipPolicy);
		return ftStep;
	}

	public static void multiThreaded(SimpleStepBuilder<?, ?> step, int threads) {
		if (threads > 1) {
			step.taskExecutor(taskExecutor(threads));
			step.throttleLimit(threads);
		}
	}

	public static TaskExecutor taskExecutor(int threads) {
		ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
		executor.setMaxPoolSize(threads);
		executor.setCorePoolSize(threads);
		executor.setQueueCapacity(threads);
		executor.afterPropertiesSet();
		return executor;
	}

	public static boolean isTerminated(JobExecution jobExecution) {
		return jobExecution.getStatus() == BatchStatus.COMPLETED
				|| jobExecution.getStatus().isGreaterThan(BatchStatus.STOPPED);
	}

	public SimpleJobLauncher getJobLauncher() {
		return jobLauncher;
	}

	public SimpleJobLauncher getAsyncJobLauncher() {
		return asyncJobLauncher;
	}

}
