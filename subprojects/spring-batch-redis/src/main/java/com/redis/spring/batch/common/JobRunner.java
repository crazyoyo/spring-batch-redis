package com.redis.spring.batch.common;

import java.time.Duration;
import java.util.concurrent.Callable;

import org.awaitility.Awaitility;
import org.awaitility.core.ConditionFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.job.builder.JobBuilderException;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.FaultTolerantStepBuilder;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.SynchronizedItemStreamReader;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.SyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

import com.redis.spring.batch.reader.PollableItemReader;
import com.redis.spring.batch.step.FlushingSimpleStepBuilder;

public class JobRunner {

	private static JobRunner memoryInstance;

	public static final Duration DEFAULT_POLL_INTERVAL = Duration.ofMillis(10);
	public static final Duration DEFAULT_RUNNING_TIMEOUT = Duration.ofSeconds(5);
	public static final Duration DEFAULT_TERMINATION_TIMEOUT = Duration.ofSeconds(5);

	private final JobRepository jobRepository;
	private final PlatformTransactionManager transactionManager;
	private final SimpleJobLauncher jobLauncher;
	private final SimpleJobLauncher asyncJobLauncher;
	private Duration pollInterval = DEFAULT_POLL_INTERVAL;
	private Duration runningTimeout = DEFAULT_RUNNING_TIMEOUT;
	private Duration terminationTimeout = DEFAULT_TERMINATION_TIMEOUT;

	public JobRunner(JobRepository jobRepository, PlatformTransactionManager transactionManager) {
		this.jobRepository = jobRepository;
		this.transactionManager = transactionManager;
		this.jobLauncher = launcher(new SyncTaskExecutor());
		this.asyncJobLauncher = launcher(new SimpleAsyncTaskExecutor());
	}

	public static JobRunner getInMemoryInstance() {
		if (memoryInstance == null) {
			try {
				memoryInstance = inMemory();
			} catch (Exception e) {
				throw new JobBuilderException(e);
			}
		}
		return memoryInstance;
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

	public JobRunner pollInterval(Duration interval) {
		this.pollInterval = interval;
		return this;
	}

	public JobRunner runningTimeout(Duration timeout) {
		this.runningTimeout = timeout;
		return this;
	}

	public JobRunner terminationTimeout(Duration timeout) {
		this.terminationTimeout = timeout;
		return this;
	}

	public JobBuilder job(String name) {
		return new JobBuilder(name).repository(jobRepository);
	}

	public <T> ItemReader<T> synchronize(ItemReader<T> reader) {
		if (reader instanceof PollableItemReader) {
			return new SynchronizedPollableItemReader<>((PollableItemReader<T>) reader);
		}
		if (reader instanceof ItemStreamReader) {
			return synchronizedItemStreamReader((ItemStreamReader<T>) reader);
		}
		return reader;
	}

	private <T> ItemReader<T> synchronizedItemStreamReader(ItemStreamReader<T> reader) {
		SynchronizedItemStreamReader<T> synchronizedReader = new SynchronizedItemStreamReader<>();
		synchronizedReader.setDelegate(reader);
		return synchronizedReader;
	}

	public static boolean isRunning(JobExecution jobExecution) {
		return jobExecution.isRunning() || jobExecution.getStatus().isUnsuccessful()
				|| jobExecution.getStatus() != BatchStatus.STARTING;
	}

	public static boolean isTerminated(JobExecution jobExecution) {
		return !jobExecution.isRunning() || jobExecution.getStatus().isUnsuccessful()
				|| jobExecution.getStatus() == BatchStatus.COMPLETED || jobExecution.getStatus() == BatchStatus.STOPPED
				|| jobExecution.getStatus().isGreaterThan(BatchStatus.STOPPED);
	}

	public SimpleJobLauncher getJobLauncher() {
		return jobLauncher;
	}

	public SimpleJobLauncher getAsyncJobLauncher() {
		return asyncJobLauncher;
	}

	public void awaitRunning(Callable<Boolean> conditionEvaluator) {
		await().timeout(runningTimeout).until(conditionEvaluator);
	}

	public void awaitRunning(JobExecution jobExecution) {
		awaitRunning(() -> isRunning(jobExecution));
	}

	private ConditionFactory await() {
		return Awaitility.await().pollInterval(pollInterval);
	}

	public void awaitTermination(Callable<Boolean> conditionEvaluator) {
		await().timeout(terminationTimeout).until(conditionEvaluator);
	}

	public void awaitTermination(JobExecution jobExecution) {
		await().timeout(terminationTimeout).until(() -> isTerminated(jobExecution));
	}

	public StepBuilder step(String name) {
		return new StepBuilder(name).repository(jobRepository).transactionManager(transactionManager);
	}

	public <I, O> SimpleStepBuilder<I, O> step(String name, ItemReader<I> reader, ItemProcessor<I, O> processor,
			ItemWriter<O> writer, StepOptions options) {
		SimpleStepBuilder<I, O> step = step(name).chunk(options.getChunkSize());
		step.reader(reader);
		step.processor(processor);
		step.writer(writer);
		if (options.getThreads() > 1) {
			step.reader(synchronize(reader));
			ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
			taskExecutor.setMaxPoolSize(options.getThreads());
			taskExecutor.setCorePoolSize(options.getThreads());
			taskExecutor.setQueueCapacity(options.getThreads());
			taskExecutor.afterPropertiesSet();
			step.taskExecutor(taskExecutor);
			step.throttleLimit(options.getThreads());
		}
		if (options.isFaultTolerant()) {
			step = faultTolerant(step, options);
		}
		if (options.getFlushingInterval().isPresent()) {
			FlushingSimpleStepBuilder<I, O> flushingStep = new FlushingSimpleStepBuilder<>(step);
			flushingStep.flushingInterval(options.getFlushingInterval().get());
			flushingStep.idleTimeout(options.getIdleTimeout());
			return flushingStep;
		}
		return step;
	}

	private <I, O> SimpleStepBuilder<I, O> faultTolerant(SimpleStepBuilder<I, O> step, StepOptions options) {
		FaultTolerantStepBuilder<I, O> ftStep = step.faultTolerant();
		ftStep.retryLimit(options.getRetryLimit());
		ftStep.skipLimit(options.getSkipLimit());
		options.getSkip().forEach(ftStep::skip);
		options.getNoSkip().forEach(ftStep::noSkip);
		options.getSkipPolicy().ifPresent(ftStep::skipPolicy);
		options.getBackOffPolicy().ifPresent(ftStep::backOffPolicy);
		options.getRetryPolicy().ifPresent(ftStep::retryPolicy);
		return ftStep;
	}

	public void awaitNotRunning(JobExecution jobExecution) {
		awaitTermination(() -> !jobExecution.isRunning());
	}

}
