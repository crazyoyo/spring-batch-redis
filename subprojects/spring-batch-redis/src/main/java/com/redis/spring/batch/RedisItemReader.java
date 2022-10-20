package com.redis.spring.batch;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionException;
import org.springframework.batch.core.job.builder.SimpleJobBuilder;
import org.springframework.batch.core.step.builder.FaultTolerantStepBuilder;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemStream;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemStreamSupport;
import org.springframework.batch.item.support.AbstractItemStreamItemReader;
import org.springframework.batch.item.support.AbstractItemStreamItemWriter;
import org.springframework.core.convert.converter.Converter;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.util.ClassUtils;

import com.redis.spring.batch.common.DataStructure;
import com.redis.spring.batch.common.JobRunner;
import com.redis.spring.batch.common.KeyDump;
import com.redis.spring.batch.common.Utils;
import com.redis.spring.batch.reader.DataStructureValueReader;
import com.redis.spring.batch.reader.KeyComparison;
import com.redis.spring.batch.reader.KeyComparisonValueReader;
import com.redis.spring.batch.reader.KeyDumpValueReader;
import com.redis.spring.batch.reader.LiveReaderBuilder;
import com.redis.spring.batch.reader.ReaderOptions;
import com.redis.spring.batch.reader.ScanReaderBuilder;
import com.redis.spring.batch.reader.StreamReaderBuilder;

import io.lettuce.core.Consumer;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;

public class RedisItemReader<K, T> extends AbstractItemStreamItemReader<T> {

	private final Log log = LogFactory.getLog(getClass());

	protected final ItemReader<K> keyReader;
	private final ItemProcessor<List<? extends K>, List<T>> valueReader;
	protected final BlockingQueue<T> queue;
	private final RedisItemReader<K, T>.Enqueuer enqueuer = new Enqueuer();
	private final JobRunner jobRunner;
	protected final ReaderOptions options;

	private JobExecution jobExecution;
	private String name;
	private final AtomicInteger runningThreads = new AtomicInteger();

	public RedisItemReader(ItemReader<K> keyReader, ItemProcessor<List<? extends K>, List<T>> valueReader,
			JobRunner jobRunner, ReaderOptions options) {
		setName(ClassUtils.getShortName(getClass()));
		this.keyReader = keyReader;
		this.valueReader = valueReader;
		this.queue = new LinkedBlockingQueue<>(options.getQueueOptions().getCapacity());
		this.jobRunner = jobRunner;
		this.options = options;
	}

	public ItemReader<K> getKeyReader() {
		return keyReader;
	}

	@Override
	public void setName(String name) {
		this.name = name;
		super.setName(name);
	}

	@Override
	public void open(ExecutionContext executionContext) throws ItemStreamException {
		synchronized (runningThreads) {
			if (jobExecution == null) {
				doOpen();
			}
			runningThreads.incrementAndGet();
			super.open(executionContext);
		}
	}

	protected void doOpen() {
		SimpleStepBuilder<K, K> step = createStep();
		FaultTolerantStepBuilder<K, K> stepBuilder = step.faultTolerant();
		options.getSkip().forEach(stepBuilder::skip);
		options.getNoSkip().forEach(stepBuilder::noSkip);
		stepBuilder.skipLimit(options.getSkipLimit());
		options.getSkipPolicy().ifPresent(stepBuilder::skipPolicy);
		if (options.getThreads() > 1) {
			ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
			taskExecutor.setMaxPoolSize(options.getThreads());
			taskExecutor.setCorePoolSize(options.getThreads());
			taskExecutor.afterPropertiesSet();
			stepBuilder.taskExecutor(taskExecutor).throttleLimit(options.getThreads());
		}
		SimpleJobBuilder simpleJobBuilder = jobRunner.job(name).start(stepBuilder.build());
		Job job = simpleJobBuilder.build();
		try {
			jobExecution = jobRunner.runAsync(job);
		} catch (JobExecutionException e) {
			throw new ItemStreamException(String.format("Could not run job %s", name), e);
		}
	}

	private class Enqueuer extends AbstractItemStreamItemWriter<K> {

		@Override
		public void open(ExecutionContext executionContext) {
			Utils.createGaugeCollectionSize("reader.queue.size", queue);
			super.open(executionContext);
			if (valueReader instanceof ItemStream) {
				((ItemStream) valueReader).open(executionContext);
			}
		}

		@Override
		public void update(ExecutionContext executionContext) {
			super.update(executionContext);
			if (valueReader instanceof ItemStream) {
				((ItemStream) valueReader).update(executionContext);
			}
		}

		@Override
		public void close() {
			if (!queue.isEmpty()) {
				log.warn("Closing with items still in queue");
			}
			if (valueReader instanceof ItemStream) {
				((ItemStream) valueReader).close();
			}
			super.close();
		}

		@Override
		public void write(List<? extends K> items) throws Exception {
			List<T> values = valueReader.process(items);
			for (T value : values) {
				queue.put(value);
			}
		}

	}

	private T poll() throws InterruptedException {
		return queue.poll(options.getQueueOptions().getPollTimeout().toMillis(), TimeUnit.MILLISECONDS);
	}

	protected SimpleStepBuilder<K, K> createStep() {
		if (keyReader instanceof ItemStreamSupport) {
			((ItemStreamSupport) keyReader).setName(name + "-reader");
		}
		enqueuer.setName(name + "-writer");
		return jobRunner.step(name).<K, K>chunk(options.getChunkSize()).reader(keyReader).writer(enqueuer);
	}

	@Override
	public T read() throws Exception {
		T item;
		do {
			item = poll();
		} while (item == null && jobExecution.isRunning() && !jobExecution.getStatus().isUnsuccessful());
		return item;
	}

	public List<T> read(int maxElements) {
		List<T> items = new ArrayList<>(maxElements);
		queue.drainTo(items, maxElements);
		return items;
	}

	@Override
	public void close() {
		super.close();
		if (runningThreads.decrementAndGet() > 0) {
			return;
		}
		synchronized (runningThreads) {
			jobRunner.awaitTermination(jobExecution);
			enqueuer.close();
			jobExecution = null;
		}
	}

	public boolean isOpen() {
		return jobExecution != null;
	}

	public static ScanReaderBuilder<String, String, KeyComparison<String>> comparator(JobRunner jobRunner,
			GenericObjectPool<StatefulConnection<String, String>> left,
			GenericObjectPool<StatefulConnection<String, String>> right, Duration ttlTolerance) {
		return new ScanReaderBuilder<>(left, jobRunner, new KeyComparisonValueReader(left, right, ttlTolerance));
	}

	public static <K, V> ScanReaderBuilder<K, V, DataStructure<K>> dataStructure(
			GenericObjectPool<StatefulConnection<K, V>> connectionPool, JobRunner jobRunner) {
		return new ScanReaderBuilder<>(connectionPool, jobRunner, new DataStructureValueReader<>(connectionPool));
	}

	public static <K, V> ScanReaderBuilder<K, V, KeyDump<K>> keyDump(
			GenericObjectPool<StatefulConnection<K, V>> connectionPool, JobRunner jobRunner) {
		return new ScanReaderBuilder<>(connectionPool, jobRunner, new KeyDumpValueReader<>(connectionPool));
	}

	public static <K, V> StreamReaderBuilder<K, V> stream(GenericObjectPool<StatefulConnection<K, V>> connectionPool,
			K name, Consumer<K> consumer) {
		return new StreamReaderBuilder<>(connectionPool, name, consumer);
	}

	public static LiveReaderBuilder<String, String, DataStructure<String>> liveDataStructure(
			GenericObjectPool<StatefulConnection<String, String>> pool, JobRunner jobRunner,
			StatefulRedisPubSubConnection<String, String> pubSubConnection, String... keyPatterns) {
		return liveDataStructure(pool, jobRunner, pubSubConnection, 0, keyPatterns);
	}

	public static LiveReaderBuilder<String, String, DataStructure<String>> liveDataStructure(
			GenericObjectPool<StatefulConnection<String, String>> pool, JobRunner jobRunner,
			StatefulRedisPubSubConnection<String, String> pubSubConnection, int database, String... keyPatterns) {
		return liveDataStructure(pool, jobRunner, pubSubConnection,
				LiveReaderBuilder.pubSubPatterns(database, keyPatterns), LiveReaderBuilder.STRING_KEY_EXTRACTOR);
	}

	public static <K, V> LiveReaderBuilder<K, V, DataStructure<K>> liveDataStructure(
			GenericObjectPool<StatefulConnection<K, V>> pool, JobRunner jobRunner,
			StatefulRedisPubSubConnection<K, V> pubSubConnection, K[] pubSubPatterns,
			Converter<K, K> eventKeyExtractor) {
		return new LiveReaderBuilder<>(jobRunner, new DataStructureValueReader<>(pool), pubSubConnection,
				pubSubPatterns, eventKeyExtractor);
	}

	public static <K, V> LiveReaderBuilder<K, V, DataStructure<K>> liveDataStructure(
			GenericObjectPool<StatefulConnection<K, V>> pool, JobRunner jobRunner,
			StatefulRedisPubSubConnection<K, V> pubSubConnection, RedisCodec<K, V> codec, int database,
			String... keyPatterns) {
		return new LiveReaderBuilder<>(jobRunner, new DataStructureValueReader<>(pool), pubSubConnection,
				LiveReaderBuilder.pubSubPatterns(codec, database, keyPatterns), LiveReaderBuilder.keyExtractor(codec));
	}

	public static <K, V> LiveReaderBuilder<K, V, KeyDump<K>> liveKeyDump(
			GenericObjectPool<StatefulConnection<K, V>> pool, JobRunner jobRunner,
			StatefulRedisPubSubConnection<K, V> pubSubConnection, RedisCodec<K, V> codec, int database,
			String... keyPatterns) {
		return new LiveReaderBuilder<>(jobRunner, new KeyDumpValueReader<>(pool), pubSubConnection,
				LiveReaderBuilder.pubSubPatterns(codec, database, keyPatterns), LiveReaderBuilder.keyExtractor(codec));
	}

	public static LiveReaderBuilder<String, String, KeyDump<String>> liveKeyDump(
			GenericObjectPool<StatefulConnection<String, String>> pool, JobRunner jobRunner,
			StatefulRedisPubSubConnection<String, String> pubSubConnection, String... keyPatterns) {
		return liveKeyDump(pool, jobRunner, pubSubConnection, 0, keyPatterns);
	}

	public static LiveReaderBuilder<String, String, KeyDump<String>> liveKeyDump(
			GenericObjectPool<StatefulConnection<String, String>> pool, JobRunner jobRunner,
			StatefulRedisPubSubConnection<String, String> pubSubConnection, int database, String... keyPatterns) {
		return liveKeyDump(pool, jobRunner, pubSubConnection, LiveReaderBuilder.pubSubPatterns(database, keyPatterns),
				LiveReaderBuilder.STRING_KEY_EXTRACTOR);
	}

	public static <K, V> LiveReaderBuilder<K, V, KeyDump<K>> liveKeyDump(
			GenericObjectPool<StatefulConnection<K, V>> pool, JobRunner jobRunner,
			StatefulRedisPubSubConnection<K, V> pubSubConnection, K[] pubSubPatterns,
			Converter<K, K> eventKeyExtractor) {
		return new LiveReaderBuilder<>(jobRunner, new KeyDumpValueReader<>(pool), pubSubConnection, pubSubPatterns,
				eventKeyExtractor);
	}

}
