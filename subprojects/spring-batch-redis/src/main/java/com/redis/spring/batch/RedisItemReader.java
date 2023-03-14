package com.redis.spring.batch;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.awaitility.Awaitility;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemStream;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.batch.item.support.AbstractItemStreamItemWriter;
import org.springframework.util.ClassUtils;

import com.redis.spring.batch.common.DataStructure;
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

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.Consumer;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.codec.RedisCodec;

public class RedisItemReader<K, T> extends AbstractItemCountingItemStreamItemReader<T> {

	public static final Duration DEFAULT_RUNNING_TIMEOUT = Duration.ofSeconds(5);

	private final JobRunner jobRunner;
	protected final ItemReader<K> keyReader;
	private final ItemProcessor<K, K> keyProcessor;
	private final ItemProcessor<List<K>, List<T>> valueReader;
	protected final StepOptions stepOptions;
	private final QueueOptions queueOptions;
	protected BlockingQueue<T> queue;
	private String name;
	private JobExecution jobExecution;

	public RedisItemReader(JobRunner jobRunner, ItemReader<K> keyReader, ItemProcessor<K, K> keyProcessor,
			ItemProcessor<List<K>, List<T>> valueReader, StepOptions stepOptions, QueueOptions queueOptions) {
		setName(ClassUtils.getShortName(getClass()));
		this.jobRunner = jobRunner;
		this.keyReader = keyReader;
		this.keyProcessor = keyProcessor;
		this.valueReader = valueReader;
		this.stepOptions = stepOptions;
		this.queueOptions = queueOptions;
	}

	@Override
	public void setName(String name) {
		super.setName(name);
		this.name = name;
	}

	@Override
	protected synchronized void doOpen() throws Exception {
		if (jobExecution == null) {
			this.queue = new LinkedBlockingQueue<>(queueOptions.getCapacity());
			Utils.createGaugeCollectionSize("reader.queue.size", queue);
			ItemWriter<K> writer = new ProcessingItemWriter<>(valueReader, new Enqueuer());
			SimpleStepBuilder<K, K> step = jobRunner.step(name, keyReader, keyProcessor, writer, stepOptions);
			Job job = jobRunner.job(name).start(step.build()).build();
			jobExecution = jobRunner.getAsyncJobLauncher().run(job, new JobParameters());
		}
	}

	private class Enqueuer extends AbstractItemStreamItemWriter<T> {

		@Override
		public void write(List<? extends T> items) throws InterruptedException {
			for (T item : items) {
				try {
					queue.put(item);
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
					throw e;
				}
			}
		}
	}

	@Override
	protected synchronized void doClose() {
		if (keyReader instanceof ItemStream) {
			((ItemStream) keyReader).close();
		}
		if (jobExecution != null) {
			Awaitility.await().until(() -> !jobExecution.isRunning());
			jobExecution = null;
		}
	}

	@Override
	protected T doRead() throws Exception {
		T item;
		do {
			item = queue.poll(queueOptions.getPollTimeout().toMillis(), TimeUnit.MILLISECONDS);
		} while (item == null && isOpen());
		return item;
	}

	public boolean isOpen() {
		return jobExecution != null && jobExecution.isRunning() && !jobExecution.getStatus().isUnsuccessful();
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

	public static <K, V> LiveReaderBuilder<K, V, DataStructure<K>> liveDataStructure(
			GenericObjectPool<StatefulConnection<K, V>> pool, JobRunner jobRunner, AbstractRedisClient client,
			RedisCodec<K, V> codec) {
		return new LiveReaderBuilder<>(jobRunner, new DataStructureValueReader<>(pool), client, codec);
	}

	public static <K, V> LiveReaderBuilder<K, V, KeyDump<K>> liveKeyDump(
			GenericObjectPool<StatefulConnection<K, V>> pool, JobRunner jobRunner, AbstractRedisClient client,
			RedisCodec<K, V> codec) {
		return new LiveReaderBuilder<>(jobRunner, new KeyDumpValueReader<>(pool), client, codec);
	}

}
