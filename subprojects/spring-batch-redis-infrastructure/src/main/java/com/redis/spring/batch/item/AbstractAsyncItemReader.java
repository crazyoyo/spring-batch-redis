package com.redis.spring.batch.item;

import java.time.Duration;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeoutException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.awaitility.Awaitility;
import org.awaitility.core.ConditionTimeoutException;
import org.springframework.batch.core.ItemReadListener;
import org.springframework.batch.core.ItemWriteListener;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionException;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.TaskExecutorJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.SynchronizedItemReader;
import org.springframework.batch.item.support.SynchronizedItemStreamReader;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.util.CollectionUtils;

import com.redis.spring.batch.JobUtils;

public abstract class AbstractAsyncItemReader<S, T> extends AbstractPollableItemReader<T> {

	public static final Duration DEFAULT_POLL_DELAY = Duration.ZERO;
	public static final Duration DEFAULT_AWAIT_POLL_INTERVAL = Duration.ofMillis(1);
	public static final Duration DEFAULT_AWAIT_TIMEOUT = Duration.ofSeconds(3);
	public static final int DEFAULT_THREADS = 1;
	public static final int DEFAULT_CHUNK_SIZE = 50;
	public static final String DEFAULT_JOB_REPOSITORY_NAME = "redis";

	private final Log log = LogFactory.getLog(getClass());

	private Duration awaitPollDelay = DEFAULT_POLL_DELAY;
	private Duration awaitPollInterval = DEFAULT_AWAIT_POLL_INTERVAL;
	private Duration awaitTimeout = DEFAULT_AWAIT_TIMEOUT;
	private int chunkSize = DEFAULT_CHUNK_SIZE;
	private int threads = DEFAULT_THREADS;
	private int skipLimit;
	private int retryLimit;
	private String jobRepositoryName = DEFAULT_JOB_REPOSITORY_NAME;
	private JobRepository jobRepository;
	private PlatformTransactionManager transactionManager = JobUtils.resourcelessTransactionManager();
	private ItemProcessor<S, S> processor;
	private Set<JobExecutionListener> jobExecutionListeners = new LinkedHashSet<>();
	private Set<ItemReadListener<S>> itemReadListeners = new LinkedHashSet<>();
	private Set<ItemWriteListener<S>> itemWriteListeners = new LinkedHashSet<>();

	private JobExecution jobExecution;
	private ItemReader<S> reader;

	@Override
	protected synchronized void doOpen() throws Exception {
		if (jobRepository == null) {
			jobRepository = JobUtils.jobRepositoryFactoryBean(jobRepositoryName).getObject();
		}
		if (jobExecution == null) {
			Job job = jobBuilder().start(step().build()).build();
			TaskExecutorJobLauncher jobLauncher = new TaskExecutorJobLauncher();
			jobLauncher.setJobRepository(jobRepository);
			jobLauncher.setTaskExecutor(new SimpleAsyncTaskExecutor());
			jobLauncher.afterPropertiesSet();
			jobExecution = jobLauncher.run(job, new JobParameters());
			try {
				awaitUntil(() -> jobRunning() || jobFailed());
			} catch (ConditionTimeoutException e) {
				List<Throwable> exceptions = jobExecution.getAllFailureExceptions();
				if (!CollectionUtils.isEmpty(exceptions)) {
					throw new JobExecutionException("Job execution unsuccessful", exceptions.get(0));
				}
			}
			Optional<Throwable> exception = JobUtils.exception(jobExecution);
			if (exception.isPresent()) {
				throw new JobExecutionException("Could not run job", exception.get());
			}
		}
	}

	private JobBuilder jobBuilder() {
		JobBuilder builder = new JobBuilder(getName(), jobRepository);
		jobExecutionListeners.forEach(builder::listener);
		return builder;
	}

	private void awaitUntil(Callable<Boolean> condition) {
		Awaitility.await().pollDelay(awaitPollDelay).pollInterval(awaitPollInterval).timeout(awaitTimeout)
				.until(condition);
	}

	protected boolean jobRunning() {
		if (jobExecution.isRunning()) {
			log.info(String.format("Job %s running", jobExecution.getJobInstance().getJobName()));
			return true;
		}
		return false;
	}

	protected boolean jobFailed() {
		if (jobExecution.getStatus().isUnsuccessful()) {
			log.warn(String.format("Job %s failed: %s", jobExecution.getJobInstance().getJobName(),
					jobExecution.getStatus()));
			return true;
		}
		return false;

	}

	@Override
	protected synchronized void doClose() throws TimeoutException, InterruptedException {
		if (jobExecution != null) {
			Awaitility.await().until(() -> !jobExecution.isRunning());
			jobExecution = null;
		}
	}

	private SimpleStepBuilder<S, S> step() {
		SimpleStepBuilder<S, S> step = stepBuilder();
		reader = reader();
		if (threads > 1) {
			step.taskExecutor(taskExecutor());
			step.throttleLimit(threads);
			step.reader(synchronize(reader));
		} else {
			step.reader(reader);
		}
		step.processor(processor);
		step.writer(writer());
		itemReadListeners.forEach(step::listener);
		itemWriteListeners.forEach(step::listener);
		return faultTolerant(step);
	}

	private TaskExecutor taskExecutor() {
		ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
		taskExecutor.setMaxPoolSize(threads);
		taskExecutor.setCorePoolSize(threads);
		taskExecutor.setQueueCapacity(threads);
		taskExecutor.afterPropertiesSet();
		return taskExecutor;
	}

	private ItemReader<? extends S> synchronize(ItemReader<S> reader) {
		if (reader instanceof ItemStreamReader) {
			SynchronizedItemStreamReader<S> synchronizedReader = new SynchronizedItemStreamReader<>();
			synchronizedReader.setDelegate((ItemStreamReader<S>) reader);
			return synchronizedReader;
		}
		return new SynchronizedItemReader<>(reader);
	}

	protected abstract ItemReader<S> reader();

	protected abstract ItemWriter<S> writer();

	protected SimpleStepBuilder<S, S> faultTolerant(SimpleStepBuilder<S, S> step) {
		if (retryLimit != 0 || skipLimit != 0) {
			return JobUtils.faultTolerant(step).retryLimit(retryLimit).skipLimit(skipLimit);
		}
		return step;
	}

	protected SimpleStepBuilder<S, S> stepBuilder() {
		return new StepBuilder(getName(), jobRepository).chunk(chunkSize, transactionManager);
	}

	@Override
	public boolean isComplete() {
		return jobExecution == null || !jobExecution.isRunning();
	}

	public void addJobExecutionListener(JobExecutionListener listener) {
		this.jobExecutionListeners.add(listener);
	}

	public void addItemReadListener(ItemReadListener<S> listener) {
		this.itemReadListeners.add(listener);
	}

	public void addItemWriteListener(ItemWriteListener<S> listener) {
		this.itemWriteListeners.add(listener);
	}

	public ItemReader<S> getReader() {
		return reader;
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

	public JobExecution getJobExecution() {
		return jobExecution;
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

	public Duration getAwaitPollDelay() {
		return awaitPollDelay;
	}

	public void setAwaitPollDelay(Duration awaitPollDelay) {
		this.awaitPollDelay = awaitPollDelay;
	}

	public Duration getAwaitPollInterval() {
		return awaitPollInterval;
	}

	public void setAwaitPollInterval(Duration awaitPollInterval) {
		this.awaitPollInterval = awaitPollInterval;
	}

	public Duration getAwaitTimeout() {
		return awaitTimeout;
	}

	public void setAwaitTimeout(Duration awaitTimeout) {
		this.awaitTimeout = awaitTimeout;
	}

}
