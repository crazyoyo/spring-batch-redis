package com.redis.spring.batch.item;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionException;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.TaskExecutorJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.SynchronizedItemReader;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.util.CollectionUtils;

import com.redis.spring.batch.Await;
import com.redis.spring.batch.JobUtils;
import com.redis.spring.batch.step.FlushingChunkProvider;
import com.redis.spring.batch.step.FlushingStepBuilder;

public abstract class AbstractAsyncItemReader<S, T> extends AbstractPollableItemReader<T> {

	public static final int DEFAULT_THREADS = 1;
	public static final int DEFAULT_CHUNK_SIZE = 50;
	public static final Duration DEFAULT_FLUSH_INTERVAL = FlushingChunkProvider.DEFAULT_FLUSH_INTERVAL;
	public static final Duration DEFAULT_IDLE_TIMEOUT = FlushingChunkProvider.DEFAULT_IDLE_TIMEOUT;
	public static final String DEFAULT_JOB_REPOSITORY_NAME = "redis";
	public static final int DEFAULT_QUEUE_CAPACITY = 10000;

	private int queueCapacity = DEFAULT_QUEUE_CAPACITY;

	private ItemReader<S> reader;
	private ItemProcessor<S, S> processor;
	private int chunkSize = DEFAULT_CHUNK_SIZE;
	private int threads = DEFAULT_THREADS;
	private int skipLimit;
	private int retryLimit;
	private Duration flushInterval = DEFAULT_FLUSH_INTERVAL;
	private Duration idleTimeout = DEFAULT_IDLE_TIMEOUT;
	private String jobRepositoryName = DEFAULT_JOB_REPOSITORY_NAME;
	private JobRepository jobRepository;
	private PlatformTransactionManager transactionManager = JobUtils.resourcelessTransactionManager();

	private BlockingQueue<T> queue;
	private JobExecution jobExecution;

	@Override
	protected synchronized void doOpen() throws Exception {
		if (queue == null) {
			queue = new LinkedBlockingQueue<>(queueCapacity);
		}
		if (jobRepository == null) {
			jobRepository = JobUtils.jobRepositoryFactoryBean(jobRepositoryName).getObject();
		}
		if (jobExecution == null) {
			SimpleStepBuilder<S, S> step = step();
			Job job = new JobBuilder(getName(), jobRepository).start(step.build()).build();
			jobExecution = runJob(job);
		}
	}

	@Override
	protected T doPoll(long timeout, TimeUnit unit) throws InterruptedException {
		return queue.poll(timeout, unit);
	}

	private JobExecution runJob(Job job) throws Exception {
		TaskExecutorJobLauncher jobLauncher = new TaskExecutorJobLauncher();
		jobLauncher.setJobRepository(jobRepository);
		jobLauncher.setTaskExecutor(new SimpleAsyncTaskExecutor());
		jobLauncher.afterPropertiesSet();
		JobExecution execution = jobLauncher.run(job, new JobParameters());
		try {
			Await.await().until(() -> execution.isRunning() || execution.getStatus().isUnsuccessful());
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw e;
		} catch (TimeoutException e) {
			List<Throwable> exceptions = execution.getAllFailureExceptions();
			if (!CollectionUtils.isEmpty(exceptions)) {
				throw new JobExecutionException("Job execution unsuccessful", exceptions.get(0));
			}
		}
		return execution;
	}

	@Override
	protected synchronized void doClose() throws TimeoutException, InterruptedException {
		if (jobExecution != null) {
			Await.await().untilFalse(jobExecution::isRunning);
			jobExecution = null;
		}
		queue = null;
	}

	private SimpleStepBuilder<S, S> step() {
		SimpleStepBuilder<S, S> step = stepBuilder();
		reader = reader();
		step.reader(reader);
		step.processor(processor);
		step.writer(writer());
		if (threads > 1) {
			ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
			taskExecutor.setMaxPoolSize(threads);
			taskExecutor.setCorePoolSize(threads);
			taskExecutor.setQueueCapacity(threads);
			taskExecutor.afterPropertiesSet();
			step.taskExecutor(taskExecutor);
			step.reader(new SynchronizedItemReader<>(reader));
		}
		return faultTolerant(step);
	}

	protected abstract ItemReader<S> reader();

	protected abstract ItemProcessor<Iterable<? extends S>, List<T>> writeProcessor();

	private ItemWriter<S> writer() {
		return new ProcessingItemWriter<>(writeProcessor(), new Writer());
	}

	private class Writer implements ItemWriter<T> {

		@Override
		public void write(Chunk<? extends T> chunk) throws InterruptedException {
			for (T element : chunk) {
				queue.put(element);
			}
		}

	}

	protected SimpleStepBuilder<S, S> faultTolerant(SimpleStepBuilder<S, S> step) {
		if (retryLimit != 0 || skipLimit != 0) {
			return JobUtils.faultTolerant(step).retryLimit(retryLimit).skipLimit(skipLimit);
		}
		return step;
	}

	private SimpleStepBuilder<S, S> stepBuilder() {
		SimpleStepBuilder<S, S> step = new StepBuilder(getName(), jobRepository).chunk(chunkSize, transactionManager);
		if (isFlushing()) {
			FlushingStepBuilder<S, S> flushingStep = new FlushingStepBuilder<>(step);
			flushingStep.flushInterval(flushInterval);
			flushingStep.idleTimeout(idleTimeout);
			return flushingStep;
		}
		return step;
	}

	@Override
	public boolean isComplete() {
		return jobExecution == null || !jobExecution.isRunning();
	}

	public BlockingQueue<T> getQueue() {
		return queue;
	}

	public int getQueueCapacity() {
		return queueCapacity;
	}

	public void setQueueCapacity(int queueCapacity) {
		this.queueCapacity = queueCapacity;
	}

	public String getJobRepositoryName() {
		return jobRepositoryName;
	}

	public void setJobRepositoryName(String name) {
		this.jobRepositoryName = name;
	}

	public JobRepository getJobRepository() {
		return jobRepository;
	}

	public void setJobRepository(JobRepository jobRepository) {
		this.jobRepository = jobRepository;
	}

	public PlatformTransactionManager getTransactionManager() {
		return transactionManager;
	}

	public void setTransactionManager(PlatformTransactionManager transactionManager) {
		this.transactionManager = transactionManager;
	}

	protected abstract boolean isFlushing();

	public JobExecution getJobExecution() {
		return jobExecution;
	}

	public ItemReader<S> getReader() {
		return reader;
	}

	public ItemProcessor<S, S> getProcessor() {
		return processor;
	}

	public void setProcessor(ItemProcessor<S, S> processor) {
		this.processor = processor;
	}

	public int getChunkSize() {
		return chunkSize;
	}

	public void setChunkSize(int chunkSize) {
		this.chunkSize = chunkSize;
	}

	public Duration getFlushInterval() {
		return flushInterval;
	}

	public void setFlushInterval(Duration interval) {
		this.flushInterval = interval;
	}

	public Duration getIdleTimeout() {
		return idleTimeout;
	}

	public void setIdleTimeout(Duration idleTimeout) {
		this.idleTimeout = idleTimeout;
	}

	public int getThreads() {
		return threads;
	}

	public void setThreads(int threads) {
		this.threads = threads;
	}

	public int getSkipLimit() {
		return skipLimit;
	}

	public void setSkipLimit(int skipLimit) {
		this.skipLimit = skipLimit;
	}

	public int getRetryLimit() {
		return retryLimit;
	}

	public void setRetryLimit(int retryLimit) {
		this.retryLimit = retryLimit;
	}

}
