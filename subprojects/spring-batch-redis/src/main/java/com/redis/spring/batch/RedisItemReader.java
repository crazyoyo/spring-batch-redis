package com.redis.spring.batch;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.FaultTolerantStepBuilder;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.core.step.skip.LimitCheckingItemSkipPolicy;
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

import com.redis.spring.batch.builder.RedisStreamItemReaderBuilder.OffsetStreamItemReaderBuilder;
import com.redis.spring.batch.builder.ScanRedisItemReaderBuilder;
import com.redis.spring.batch.support.DataStructure;
import com.redis.spring.batch.support.DataStructureValueReader;
import com.redis.spring.batch.support.DataStructureValueReader.DataStructureValueReaderFactory;
import com.redis.spring.batch.support.KeyDumpValueReader;
import com.redis.spring.batch.support.KeyDumpValueReader.KeyDumpValueReaderFactory;
import com.redis.spring.batch.support.KeyValue;
import com.redis.spring.batch.support.RedisValueEnqueuer;
import com.redis.spring.batch.support.Utils;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.RedisCommandTimeoutException;
import io.lettuce.core.XReadArgs.StreamOffset;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RedisItemReader<K, T extends KeyValue<K, ?>> extends AbstractItemStreamItemReader<T> {

	public static final int DEFAULT_THREADS = 1;
	public static final int DEFAULT_CHUNK_SIZE = 50;
	public static final int DEFAULT_QUEUE_CAPACITY = 10000;
	public static final Duration DEFAULT_QUEUE_POLL_TIMEOUT = Duration.ofMillis(100);
	public static final int DEFAULT_SKIP_LIMIT = 3;
	public static final Map<Class<? extends Throwable>, Boolean> DEFAULT_SKIPPABLE_EXCEPTIONS = Stream
			.of(RedisCommandExecutionException.class, RedisCommandTimeoutException.class, TimeoutException.class)
			.collect(Collectors.toMap(t -> t, t -> true));
	public static final SkipPolicy DEFAULT_SKIP_POLICY = new LimitCheckingItemSkipPolicy(DEFAULT_SKIP_LIMIT,
			DEFAULT_SKIPPABLE_EXCEPTIONS);

	private final JobRepository jobRepository;
	private final PlatformTransactionManager transactionManager;
	private final ItemReader<K> keyReader;
	private final ItemProcessor<List<? extends K>, List<T>> valueReader;

	private int threads = DEFAULT_THREADS;
	private int chunkSize = DEFAULT_CHUNK_SIZE;
	private int queueCapacity = DEFAULT_QUEUE_CAPACITY;
	private Duration queuePollTimeout = DEFAULT_QUEUE_POLL_TIMEOUT;
	private SkipPolicy skipPolicy = DEFAULT_SKIP_POLICY;

	protected BlockingQueue<T> valueQueue;
	protected RedisValueEnqueuer<K, T> enqueuer;
	private JobExecution jobExecution;
	private String name;

	public RedisItemReader(JobRepository jobRepository, PlatformTransactionManager transactionManager,
			ItemReader<K> keyReader, ItemProcessor<List<? extends K>, List<T>> valueReader) {
		setName(ClassUtils.getShortName(getClass()));
		Assert.notNull(jobRepository, "A job repository is required");
		Assert.notNull(transactionManager, "A platform transaction manager is required");
		Assert.notNull(keyReader, "A key reader is required");
		Assert.notNull(valueReader, "A value reader is required");
		this.jobRepository = jobRepository;
		this.transactionManager = transactionManager;
		this.keyReader = keyReader;
		this.valueReader = valueReader;
	}

	public void setThreads(int threads) {
		Utils.assertPositive(threads, "Thread count");
		this.threads = threads;
	}

	public void setChunkSize(int chunkSize) {
		Utils.assertPositive(chunkSize, "Chunk size");
		this.chunkSize = chunkSize;
	}

	public void setQueueCapacity(int queueCapacity) {
		Utils.assertPositive(queueCapacity, "Value queue capacity");
		this.queueCapacity = queueCapacity;
	}

	public void setQueuePollTimeout(Duration queuePollTimeout) {
		Utils.assertPositive(queuePollTimeout, "Queue poll timeout");
		this.queuePollTimeout = queuePollTimeout;
	}

	public void setSkipPolicy(SkipPolicy skipPolicy) {
		Assert.notNull(skipPolicy, "A skip policy is required");
		this.skipPolicy = skipPolicy;
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
		valueQueue = new LinkedBlockingQueue<>(queueCapacity);
		enqueuer = new RedisValueEnqueuer<>(valueReader, valueQueue);
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
			item = valueQueue.poll(queuePollTimeout.toMillis(), TimeUnit.MILLISECONDS);
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
		return new ScanRedisItemReaderBuilder<>(jobRepository, transactionManager, client,
				new DataStructureValueReaderFactory<>());
	}

	public static ScanRedisItemReaderBuilder<KeyValue<String, byte[]>, KeyDumpValueReader<String, String>> keyDump(
			JobRepository jobRepository, PlatformTransactionManager transactionManager, AbstractRedisClient client) {
		return new ScanRedisItemReaderBuilder<>(jobRepository, transactionManager, client,
				new KeyDumpValueReaderFactory<>());
	}

	public static OffsetStreamItemReaderBuilder stream(StreamOffset<String> offset) {
		return new OffsetStreamItemReaderBuilder(offset);
	}

}
