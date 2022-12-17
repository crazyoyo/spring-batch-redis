package com.redis.spring.batch.common;

import java.time.Duration;
import java.util.Optional;

import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.FaultTolerantStepBuilder;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.SyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

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

	public static JobRunner inMemory() throws Exception {
		@SuppressWarnings("deprecation")
		org.springframework.batch.core.repository.support.MapJobRepositoryFactoryBean bean = new org.springframework.batch.core.repository.support.MapJobRepositoryFactoryBean();
		bean.afterPropertiesSet();
		return new JobRunner(bean.getObject(), bean.getTransactionManager());
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

	public <I, O> SimpleStepBuilder<I, O> step(String name, ItemReader<I> reader, ItemProcessor<I, O> processor,
			ItemWriter<O> writer, StepOptions options) {
		SimpleStepBuilder<I, O> builder = step(name).<I, O>chunk(options.getChunkSize()).reader(reader)
				.processor(processor).writer(writer);
		Optional<Duration> flushingInterval = options.getFlushingInterval();
		if (flushingInterval.isPresent()) {
			FlushingSimpleStepBuilder<I, O> flushingStep = new FlushingSimpleStepBuilder<>(builder);
			flushingStep.flushingInterval(flushingInterval.get().toMillis());
			options.getIdleTimeout().map(Duration::toMillis).ifPresent(flushingStep::idleTimeout);
			builder = flushingStep;
		}
		if (options.getThreads() > 1) {
			ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
			executor.setMaxPoolSize(options.getThreads());
			executor.setCorePoolSize(options.getThreads());
			executor.afterPropertiesSet();
			builder.taskExecutor(executor).throttleLimit(options.getThreads());
		}
		if (options.isFaultTolerant()) {
			FaultTolerantStepBuilder<I, O> faultTolerantStep = builder.faultTolerant();
			options.getSkip().forEach(faultTolerantStep::skip);
			options.getNoSkip().forEach(faultTolerantStep::noSkip);
			faultTolerantStep.skipLimit(options.getSkipLimit());
			options.getSkipPolicy().ifPresent(faultTolerantStep::skipPolicy);
			builder = faultTolerantStep;
		}
		return builder;
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
