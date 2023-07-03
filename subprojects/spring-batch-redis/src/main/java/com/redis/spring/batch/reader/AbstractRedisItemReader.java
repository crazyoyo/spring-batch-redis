package com.redis.spring.batch.reader;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionException;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.support.AbstractItemStreamItemReader;
import org.springframework.batch.support.transaction.ResourcelessTransactionManager;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.util.ClassUtils;

import com.redis.spring.batch.common.KeyValue;
import com.redis.spring.batch.common.Openable;
import com.redis.spring.batch.common.OperationItemStreamSupport;
import com.redis.spring.batch.common.ProcessingItemWriter;
import com.redis.spring.batch.common.QueueItemWriter;
import com.redis.spring.batch.common.SimpleBatchOperation;
import com.redis.spring.batch.common.Utils;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.codec.RedisCodec;

public abstract class AbstractRedisItemReader<K, V, T extends KeyValue<K>> extends AbstractItemStreamItemReader<T>
		implements PollableItemReader<T>, Openable {

	protected final AbstractRedisClient client;
	protected final RedisCodec<K, V> codec;
	private final AbstractLuaReadOperation<K, V, T> operation;
	private ItemProcessor<K, K> processor;

	private JobRepository jobRepository;
	protected ReaderOptions options = ReaderOptions.builder().build();
	private String name;
	private JobExecution jobExecution;
	private BlockingQueue<T> queue;

	protected AbstractRedisItemReader(AbstractRedisClient client, RedisCodec<K, V> codec,
			AbstractLuaReadOperation<K, V, T> operation) {
		setName(ClassUtils.getShortName(getClass()));
		this.client = client;
		this.codec = codec;
		this.operation = operation;
	}

	public void setOptions(ReaderOptions options) {
		this.options = options;
	}

	public void setJobRepository(JobRepository jobRepository) {
		this.jobRepository = jobRepository;
	}

	public void setKeyProcessor(ItemProcessor<K, K> processor) {
		this.processor = processor;
	}

	@Override
	public void setName(String name) {
		super.setName(name);
		this.name = name;
	}

	@Override
	public synchronized void open(ExecutionContext executionContext) {
		super.open(executionContext);
		if (queue != null) {
			return;
		}
		OperationItemStreamSupport<K, V, K, T> operationProcessor = operationProcessor();
		queue = queue();
		if (jobRepository == null) {
			try {
				jobRepository = Utils.inMemoryJobRepository();
			} catch (Exception e) {
				throw new ItemStreamException("Could not initialize job repository", e);
			}
		}
		StepBuilder stepBuilder = new StepBuilder(name + "-step");
		stepBuilder.repository(jobRepository);
		stepBuilder.transactionManager(transactionManager());
		SimpleStepBuilder<K, K> step = step(stepBuilder);
		ItemReader<K> reader = keyReader();
		step.reader(options.getThreads() > 1 ? Utils.synchronizedReader(reader) : reader);
		step.processor(processor);
		QueueItemWriter<T> queueWriter = new QueueItemWriter<>(queue);
		ProcessingItemWriter<K, T> writer = new ProcessingItemWriter<>(operationProcessor, queueWriter);
		step.writer(writer);
		Utils.multiThread(step, options.getThreads());
		JobBuilderFactory jobBuilderFactory = new JobBuilderFactory(jobRepository);
		Job job = jobBuilderFactory.get(name).start(step.build()).build();
		SimpleJobLauncher jobLauncher = new SimpleJobLauncher();
		jobLauncher.setJobRepository(jobRepository);
		jobLauncher.setTaskExecutor(new SimpleAsyncTaskExecutor());
		try {
			jobExecution = jobLauncher.run(job, new JobParameters());
		} catch (JobExecutionException e) {
			throw new ItemStreamException("Job execution failed", e);
		}
		while (!jobExecution.isRunning() && !jobExecution.getStatus().isUnsuccessful()
				&& jobExecution.getStatus() != BatchStatus.COMPLETED) {
			sleep();
		}
		sleep();
		if (jobExecution.getStatus().isUnsuccessful()) {
			throw new ItemStreamException("Could not run job",
					jobExecution.getAllFailureExceptions().iterator().next());
		}
	}

	public OperationItemStreamSupport<K, V, K, T> operationProcessor() {
		operation.setMemoryUsageOptions(options.getMemoryUsageOptions());
		OperationItemStreamSupport<K, V, K, T> operationProcessor = new OperationItemStreamSupport<>(client, codec,
				new SimpleBatchOperation<>(operation));
		operationProcessor.setPoolOptions(options.getPoolOptions());
		operationProcessor.setReadFrom(options.getReadFrom());
		return operationProcessor;
	}

	protected abstract ItemReader<K> keyReader();

	private void sleep() {
		try {
			Thread.sleep(options.getQueueOptions().getPollTimeout().toMillis());
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new ItemStreamException("Interrupted during initialization", e);
		}
	}

	private PlatformTransactionManager transactionManager() {
		return new ResourcelessTransactionManager();
	}

	public JobExecution getJobExecution() {
		return jobExecution;
	}

	protected SimpleStepBuilder<K, K> step(StepBuilder step) {
		return step.chunk(options.getChunkSize());
	}

	private BlockingQueue<T> queue() {
		BlockingQueue<T> blockingQueue = new LinkedBlockingQueue<>(options.getQueueOptions().getCapacity());
		Utils.createGaugeCollectionSize("reader.queue.size", blockingQueue);
		return blockingQueue;
	}

	@Override
	public synchronized void close() {
		if (jobExecution != null) {
			if (jobExecution.isRunning()) {
				for (StepExecution stepExecution : jobExecution.getStepExecutions()) {
					stepExecution.setTerminateOnly();
				}
				jobExecution.setStatus(BatchStatus.STOPPING);
			}
			jobExecution = null;
		}
		queue = null;
		super.close();
	}

	@Override
	public boolean isOpen() {
		return jobExecution != null;
	}

	@Override
	public synchronized T poll(long timeout, TimeUnit unit) throws InterruptedException {
		return queue.poll(timeout, unit);
	}

	@Override
	public synchronized T read() throws Exception {
		T item;
		do {
			item = queue.poll(options.getQueueOptions().getPollTimeout().toMillis(), TimeUnit.MILLISECONDS);
		} while (item == null && jobExecution != null && jobExecution.isRunning());
		if (jobExecution != null && jobExecution.getStatus().isUnsuccessful()) {
			throw new ItemStreamException("Reader job failed");
		}
		return item;
	}

	public synchronized List<T> read(int maxElements) {
		List<T> items = new ArrayList<>(maxElements);
		queue.drainTo(items, maxElements);
		return items;
	}

}
