package com.redis.spring.batch;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.AbstractItemStreamItemWriter;
import org.springframework.core.convert.converter.Converter;

import com.redis.spring.batch.common.DataStructure;
import com.redis.spring.batch.common.JobExecutionItemStream;
import com.redis.spring.batch.common.JobRunner;
import com.redis.spring.batch.common.KeyDump;
import com.redis.spring.batch.common.ProcessingItemWriter;
import com.redis.spring.batch.common.StepOptions;
import com.redis.spring.batch.common.Utils;
import com.redis.spring.batch.reader.DataStructureValueReader;
import com.redis.spring.batch.reader.KeyComparison;
import com.redis.spring.batch.reader.KeyComparisonValueReader;
import com.redis.spring.batch.reader.KeyDumpValueReader;
import com.redis.spring.batch.reader.LiveReaderBuilder;
import com.redis.spring.batch.reader.QueueOptions;
import com.redis.spring.batch.reader.ScanReaderBuilder;
import com.redis.spring.batch.reader.StreamReaderBuilder;

import io.lettuce.core.Consumer;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;

public class RedisItemReader<K, T> extends JobExecutionItemStream implements ItemStreamReader<T> {

	protected final ItemReader<K> keyReader;
	private final ItemProcessor<List<K>, List<T>> valueReader;
	protected final StepOptions stepOptions;
	private final QueueOptions queueOptions;
	protected final BlockingQueue<T> queue;

	public RedisItemReader(JobRunner jobRunner, ItemReader<K> keyReader, ItemProcessor<List<K>, List<T>> valueReader,
			StepOptions stepOptions, QueueOptions queueOptions) {
		super(jobRunner);
		this.keyReader = keyReader;
		this.valueReader = valueReader;
		this.stepOptions = stepOptions;
		this.queueOptions = queueOptions;
		this.queue = new LinkedBlockingQueue<>(queueOptions.getCapacity());
	}

	public ItemReader<K> getKeyReader() {
		return keyReader;
	}

	@Override
	protected Job job() {
		Utils.createGaugeCollectionSize("reader.queue.size", queue);
		ItemWriter<K> writer = new ProcessingItemWriter<>(valueReader, new QueueWriter<>(queue));
		TaskletStep step = jobRunner.step(name, keyReader, null, writer, stepOptions).build();
		return jobRunner.job(name).start(step).build();
	}

	private static class QueueWriter<T> extends AbstractItemStreamItemWriter<T> {

		private final Log log = LogFactory.getLog(getClass());

		private final BlockingQueue<T> queue;

		public QueueWriter(BlockingQueue<T> queue) {
			this.queue = queue;
		}

		@Override
		public void close() {
			if (!queue.isEmpty()) {
				log.warn("Closing with items still in queue");
			}
			super.close();
		}

		@Override
		public void write(List<? extends T> items) throws Exception {
			for (T item : items) {
				queue.put(item);
			}
		}

	}

	private T poll() throws InterruptedException {
		return queue.poll(queueOptions.getPollTimeout().toMillis(), TimeUnit.MILLISECONDS);
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
			StatefulRedisPubSubConnection<K, V> pubSubConnection, K[] pubSubPatterns, Converter<K, K> keyExtractor) {
		return new LiveReaderBuilder<>(jobRunner, new DataStructureValueReader<>(pool), pubSubConnection,
				pubSubPatterns, keyExtractor);
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
