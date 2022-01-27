package com.redis.spring.batch;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionException;
import org.springframework.batch.core.job.builder.SimpleJobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.FaultTolerantStepBuilder;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.core.step.skip.LimitCheckingItemSkipPolicy;
import org.springframework.batch.core.step.skip.SkipPolicy;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.support.AbstractItemStreamItemReader;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import com.redis.spring.batch.reader.DataStructureIntrospectingValueReader;
import com.redis.spring.batch.reader.DataStructureValueReader;
import com.redis.spring.batch.reader.KeyDumpValueReader;
import com.redis.spring.batch.reader.RedisValueEnqueuer;
import com.redis.spring.batch.reader.ScanRedisItemReaderBuilder;
import com.redis.spring.batch.reader.StreamItemReaderBuilder;
import com.redis.spring.batch.reader.ValueReader;
import com.redis.spring.batch.support.JobRunner;
import com.redis.spring.batch.support.Utils;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.RedisCommandTimeoutException;
import io.lettuce.core.cluster.RedisClusterClient;

public class RedisItemReader<K, T extends KeyValue<K, ?>> extends AbstractItemStreamItemReader<T> {

	private static final Logger log = LoggerFactory.getLogger(RedisItemReader.class);

	public static final int DEFAULT_THREADS = 1;
	public static final int DEFAULT_CHUNK_SIZE = 50;
	public static final int DEFAULT_QUEUE_CAPACITY = 10000;
	public static final Duration DEFAULT_QUEUE_POLL_TIMEOUT = Duration.ofMillis(100);
	public static final int DEFAULT_SKIP_LIMIT = 3;
	public static final SkipPolicy DEFAULT_SKIP_POLICY = limitCheckingSkipPolicy(DEFAULT_SKIP_LIMIT);

	private static final long JOB_EXECUTION_WAIT = 100;

	private final AtomicInteger threadCount = new AtomicInteger();
	private final ItemReader<K> keyReader;
	private final ValueReader<K, T> valueReader;
	protected final BlockingQueue<T> valueQueue;
	protected final RedisValueEnqueuer<K, T> enqueuer;
	private final JobRunner jobRunner;

	private int threads = DEFAULT_THREADS;
	private int chunkSize = DEFAULT_CHUNK_SIZE;
	private int queueCapacity = DEFAULT_QUEUE_CAPACITY;
	private Duration queuePollTimeout = DEFAULT_QUEUE_POLL_TIMEOUT;
	private SkipPolicy skipPolicy = DEFAULT_SKIP_POLICY;
	private JobExecution jobExecution;
	private String name;
	private boolean open;

	public RedisItemReader(JobRepository jobRepository, PlatformTransactionManager transactionManager,
			ItemReader<K> keyReader, ValueReader<K, T> valueReader) {
		setName(ClassUtils.getShortName(getClass()));
		Assert.notNull(jobRepository, "A job repository is required");
		Assert.notNull(transactionManager, "A platform transaction manager is required");
		Assert.notNull(keyReader, "A key reader is required");
		Assert.notNull(valueReader, "A value reader is required");
		this.keyReader = keyReader;
		this.valueReader = valueReader;
		this.valueQueue = new LinkedBlockingQueue<>(queueCapacity);
		this.enqueuer = new RedisValueEnqueuer<>(valueReader, valueQueue);
		this.jobRunner = new JobRunner(jobRepository, transactionManager);
	}

	public static SkipPolicy limitCheckingSkipPolicy(int skipLimit) {
		return new LimitCheckingItemSkipPolicy(skipLimit, Stream
				.of(RedisCommandExecutionException.class, RedisCommandTimeoutException.class, TimeoutException.class)
				.collect(Collectors.toMap(t -> t, t -> true)));
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

	public ValueReader<K, T> getValueReader() {
		return valueReader;
	}

	@Override
	public void setName(String name) {
		this.name = name;
		super.setName(name);
	}

	@Override
	public void open(ExecutionContext executionContext) throws ItemStreamException {
		synchronized (threadCount) {
			if (jobExecution == null) {
				Utils.createGaugeCollectionSize("reader.queue.size", valueQueue);
				FaultTolerantStepBuilder<K, K> stepBuilder = faultTolerant(jobRunner.step(name).chunk(chunkSize));
				stepBuilder.skipPolicy(skipPolicy);
				stepBuilder.reader(keyReader).writer(enqueuer);
				if (threads > 1) {
					ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
					taskExecutor.setMaxPoolSize(threads);
					taskExecutor.setCorePoolSize(threads);
					taskExecutor.afterPropertiesSet();
					stepBuilder.taskExecutor(taskExecutor).throttleLimit(threads);
				}
				SimpleJobBuilder simpleJobBuilder = jobRunner.job(name).start(stepBuilder.build());
				try {
					log.debug("Executing job {}", name);
					jobExecution = jobRunner.runAsync(simpleJobBuilder.build());
					while (jobExecution.getStatus() == BatchStatus.STARTING) {
						threadCount.wait(JOB_EXECUTION_WAIT);
					}
					log.debug("Job {} status: {}", name, jobExecution.getStatus());
					if (jobExecution.getStatus().isUnsuccessful()) {
						List<Throwable> failureExceptions = jobExecution.getAllFailureExceptions();
						String message = String.format("Could not run job %s", name);
						if (failureExceptions.isEmpty()) {
							throw new ItemStreamException(message);
						}
						throw new ItemStreamException(message, failureExceptions.get(0));
					}
				} catch (InterruptedException e) {
					log.debug("Interrupted while waiting for job {} to run", name, e);
					Thread.currentThread().interrupt();
				} catch (JobExecutionException e) {
					throw new ItemStreamException(String.format("Could not run job %s", name), e);
				}
				open = true;
			}
			threadCount.incrementAndGet();
			super.open(executionContext);
		}
	}

	protected FaultTolerantStepBuilder<K, K> faultTolerant(SimpleStepBuilder<K, K> stepBuilder) {
		return stepBuilder.faultTolerant();
	}

	@Override
	public T read() throws Exception {
		T item;
		do {
			item = valueQueue.poll(queuePollTimeout.toMillis(), TimeUnit.MILLISECONDS);
		} while (item == null && jobExecution.isRunning() && !jobExecution.getStatus().isUnsuccessful());
		return item;
	}

	public List<T> read(int maxElements) {
		List<T> items = new ArrayList<>(maxElements);
		valueQueue.drainTo(items, maxElements);
		return items;
	}

	@Override
	public void close() {
		super.close();
		if (threadCount.decrementAndGet() > 0) {
			return;
		}
		synchronized (threadCount) {
			log.debug("Closing {}", name);
			while (jobExecution.isRunning() && !jobExecution.getStatus().isUnsuccessful()) {
				try {
					threadCount.wait(JOB_EXECUTION_WAIT);
				} catch (InterruptedException e) {
					log.debug("Interrupted while waiting for job {} to complete", name, e);
					Thread.currentThread().interrupt();
				}
			}
			if (!valueQueue.isEmpty()) {
				log.warn("Closing {} with {} items still in queue", ClassUtils.getShortName(getClass()),
						valueQueue.size());
			}
			open = false;
		}
	}

	public boolean isOpen() {
		return open;
	}

	public static Builder client(RedisClient client) {
		return new Builder(client);
	}

	public static Builder client(RedisClusterClient client) {
		return new Builder(client);
	}

	public static class Builder {

		private final AbstractRedisClient client;

		public Builder(AbstractRedisClient client) {
			this.client = client;
		}

		public ScanRedisItemReaderBuilder<DataStructure<String>, DataStructureValueReader<String, String>> dataStructure() {
			return new ScanRedisItemReaderBuilder<>(client, DataStructureValueReader::new);
		}

		public ScanRedisItemReaderBuilder<DataStructure<String>, DataStructureValueReader<String, String>> dataStructureIntrospect() {
			return new ScanRedisItemReaderBuilder<>(client, DataStructureIntrospectingValueReader::new);
		}

		public ScanRedisItemReaderBuilder<KeyValue<String, byte[]>, KeyDumpValueReader<String, String>> keyDump() {
			return new ScanRedisItemReaderBuilder<>(client, KeyDumpValueReader::new);
		}

		public StreamItemReaderBuilder stream(String name) {
			return new StreamItemReaderBuilder(client, name);
		}

	}

}
