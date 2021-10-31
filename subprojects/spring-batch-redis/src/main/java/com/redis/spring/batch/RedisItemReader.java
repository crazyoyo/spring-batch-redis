package com.redis.spring.batch;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.FaultTolerantStepBuilder;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.core.step.skip.SkipPolicy;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.support.AbstractItemStreamItemReader;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import com.redis.spring.batch.support.DataStructure;
import com.redis.spring.batch.support.DataStructureValueReader;
import com.redis.spring.batch.support.DataStructureValueReader.DataStructureValueReaderFactory;
import com.redis.spring.batch.support.KeyDumpValueReader;
import com.redis.spring.batch.support.KeyDumpValueReader.KeyDumpValueReaderFactory;
import com.redis.spring.batch.support.KeyValue;
import com.redis.spring.batch.support.RedisItemReaderBuilder.ScanRedisItemReaderBuilder;
import com.redis.spring.batch.support.RedisStreamItemReaderBuilder.OffsetStreamItemReaderBuilder;
import com.redis.spring.batch.support.RedisValueEnqueuer;
import com.redis.spring.batch.support.Utils;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.XReadArgs.StreamOffset;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RedisItemReader<K, T extends KeyValue<K, ?>> extends AbstractItemStreamItemReader<T> {

	private final JobRepository jobRepository;
	private final PlatformTransactionManager transactionManager;
	private final ItemReader<K> keyReader;
	private final ItemProcessor<List<? extends K>, List<T>> valueReader;
	protected final RedisValueEnqueuer<K, T> enqueuer;
	private final int threads;
	private final int chunkSize;
	private final SkipPolicy skipPolicy;
	private final long pollTimeout;

	protected BlockingQueue<T> valueQueue;
	private JobExecution jobExecution;
	private String name;

	public RedisItemReader(JobRepository jobRepository, PlatformTransactionManager transactionManager,
			ItemReader<K> keyReader, ItemProcessor<List<? extends K>, List<T>> valueReader, int threads, int chunkSize,
			BlockingQueue<T> valueQueue, Duration queuePollTimeout, SkipPolicy skipPolicy) {
		setName(ClassUtils.getShortName(getClass()));
		Assert.notNull(jobRepository, "A job repository is required");
		Assert.notNull(transactionManager, "A platform transaction manager is required");
		Assert.notNull(keyReader, "A key reader is required");
		Assert.notNull(valueReader, "A value reader is required");
		Utils.assertPositive(threads, "Thread count");
		Utils.assertPositive(chunkSize, "Chunk size");
		Assert.notNull(valueQueue, "A queue is required");
		Utils.assertPositive(queuePollTimeout, "Queue poll timeout");
		Assert.notNull(skipPolicy, "A skip policy is required");
		this.jobRepository = jobRepository;
		this.transactionManager = transactionManager;
		this.keyReader = keyReader;
		this.valueReader = valueReader;
		this.threads = threads;
		this.chunkSize = chunkSize;
		this.valueQueue = valueQueue;
		this.pollTimeout = queuePollTimeout.toMillis();
		this.skipPolicy = skipPolicy;
		this.enqueuer = new RedisValueEnqueuer<>(valueReader, valueQueue);
	}

	public ItemProcessor<List<? extends K>, List<T>> getValueReader() {
		return valueReader;
	}

	@Override
	public void setName(String name) {
		this.name = name;
		super.setName(name);
	}

	@Override
	public synchronized void open(ExecutionContext executionContext) throws ItemStreamException {
		if (jobExecution != null) {
			log.info("Already opened, skipping");
			return;
		}
		log.info("Opening {}", name);
		Utils.createGaugeCollectionSize("reader.queue.size", valueQueue);
		StepBuilderFactory stepBuilderFactory = new StepBuilderFactory(jobRepository, transactionManager);
		FaultTolerantStepBuilder<K, K> stepBuilder = faultTolerantStepBuilder(
				stepBuilderFactory.get(name).chunk(chunkSize));
		stepBuilder.skipPolicy(skipPolicy);
		stepBuilder.reader(keyReader).writer(enqueuer);
		if (threads > 1) {
			ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
			taskExecutor.setMaxPoolSize(threads);
			taskExecutor.setCorePoolSize(threads);
			taskExecutor.afterPropertiesSet();
			stepBuilder.taskExecutor(taskExecutor).throttleLimit(threads);
		}
		JobBuilderFactory jobBuilderFactory = new JobBuilderFactory(jobRepository);
		Job job = jobBuilderFactory.get(name).start(stepBuilder.build()).build();
		SimpleJobLauncher jobLauncher = new SimpleJobLauncher();
		jobLauncher.setJobRepository(jobRepository);
		jobLauncher.setTaskExecutor(new SimpleAsyncTaskExecutor());
		try {
			jobExecution = jobLauncher.run(job, new JobParameters());
		} catch (Exception e) {
			throw new ItemStreamException("Could not run job for reader " + name, e);
		}
		super.open(executionContext);
	}

	protected FaultTolerantStepBuilder<K, K> faultTolerantStepBuilder(SimpleStepBuilder<K, K> stepBuilder) {
		return stepBuilder.faultTolerant();
	}

	@Override
	public T read() throws Exception {
		T item;
		do {
			item = valueQueue.poll(pollTimeout, TimeUnit.MILLISECONDS);
		} while (item == null && jobExecution.isRunning());
		return item;
	}

	public List<T> read(int maxElements) {
		List<T> items = new ArrayList<>(maxElements);
		valueQueue.drainTo(items, maxElements);
		return items;
	}

	@Override
	public synchronized void close() {
		if (jobExecution == null) {
			log.info("Already closed, skipping");
			return;
		}
		log.info("Closing {}", name);
		super.close();
		if (!valueQueue.isEmpty()) {
			log.warn("Closing {} with {} items still in queue", ClassUtils.getShortName(getClass()), valueQueue.size());
		}
		jobExecution = null;
	}

	public static ScanRedisItemReaderBuilder<DataStructure<String>, DataStructureValueReader<String, String>> dataStructure(
			JobRepository jobRepository, PlatformTransactionManager transactionManager, AbstractRedisClient client) {
		return new ScanRedisItemReaderBuilder<DataStructure<String>, DataStructureValueReader<String, String>>(
				jobRepository, transactionManager, client, new DataStructureValueReaderFactory<String, String>());
	}

	public static ScanRedisItemReaderBuilder<KeyValue<String, byte[]>, KeyDumpValueReader<String, String>> keyDump(
			JobRepository jobRepository, PlatformTransactionManager transactionManager, AbstractRedisClient client) {
		return new ScanRedisItemReaderBuilder<KeyValue<String, byte[]>, KeyDumpValueReader<String, String>>(
				jobRepository, transactionManager, client, new KeyDumpValueReaderFactory<String, String>());
	}

	public static OffsetStreamItemReaderBuilder stream(StreamOffset<String> offset) {
		return new OffsetStreamItemReaderBuilder(offset);
	}

}
