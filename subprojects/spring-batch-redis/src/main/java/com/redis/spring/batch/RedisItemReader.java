package com.redis.spring.batch;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.step.builder.FaultTolerantStepBuilder;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.core.step.skip.SkipPolicy;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemStream;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.AbstractItemStreamItemReader;
import org.springframework.batch.item.support.AbstractItemStreamItemWriter;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import com.redis.spring.batch.support.DataStructure;
import com.redis.spring.batch.support.DataStructureValueReader;
import com.redis.spring.batch.support.DataStructureValueReader.DataStructureValueReaderFactory;
import com.redis.spring.batch.support.KeyDumpValueReader;
import com.redis.spring.batch.support.KeyDumpValueReader.KeyDumpValueReaderFactory;
import com.redis.spring.batch.support.KeyValue;
import com.redis.spring.batch.support.ScanRedisItemReaderBuilder;
import com.redis.spring.batch.support.RedisStreamItemReaderBuilder.OffsetStreamItemReaderBuilder;
import com.redis.spring.batch.support.job.JobExecutionWrapper;
import com.redis.spring.batch.support.job.JobFactory;
import com.redis.spring.batch.support.Utils;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.XReadArgs.StreamOffset;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RedisItemReader<K, T extends KeyValue<K, ?>> extends AbstractItemStreamItemReader<T> {

	private final ItemReader<K> keyReader;
	private final ItemProcessor<List<? extends K>, List<T>> valueReader;
	private final int threads;
	private final int chunkSize;
	private final SkipPolicy skipPolicy;
	private final long pollTimeout;

	protected BlockingQueue<T> queue;
	private JobExecutionWrapper jobExecution;
	private String name;

	public RedisItemReader(ItemReader<K> keyReader, ItemProcessor<List<? extends K>, List<T>> valueReader, int threads,
			int chunkSize, BlockingQueue<T> queue, Duration queuePollTimeout, SkipPolicy skipPolicy) {
		setName(ClassUtils.getShortName(getClass()));
		Assert.notNull(keyReader, "A key reader is required");
		Assert.notNull(valueReader, "A value reader is required");
		Utils.assertPositive(threads, "Thread count");
		Utils.assertPositive(chunkSize, "Chunk size");
		Assert.notNull(queue, "A queue is required");
		Utils.assertPositive(queuePollTimeout, "Queue poll timeout");
		Assert.notNull(skipPolicy, "A skip policy is required");
		this.keyReader = keyReader;
		this.valueReader = valueReader;
		this.threads = threads;
		this.chunkSize = chunkSize;
		this.queue = queue;
		this.pollTimeout = queuePollTimeout.toMillis();
		this.skipPolicy = skipPolicy;
	}

	@Override
	public void setName(String name) {
		this.name = name;
		super.setName(name);
	}

	@Override
	public synchronized void open(ExecutionContext executionContext) throws ItemStreamException {
		if (jobExecution != null) {
			log.debug("Already opened, skipping");
			return;
		}
		log.debug("Opening {}", name);
		Utils.createGaugeCollectionSize("reader.queue.size", queue);
		ItemWriter<K> writer = new ValueWriter();
		JobFactory factory;
		try {
			factory = JobFactory.inMemory();
		} catch (Exception e) {
			throw new ItemStreamException("Failed to initialize the reader", e);
		}
		FaultTolerantStepBuilder<K, K> stepBuilder = faultTolerantStepBuilder(
				factory.step(name + "-step").chunk(chunkSize));
		stepBuilder.skipPolicy(skipPolicy);
		stepBuilder.reader(keyReader).writer(writer);
		if (threads > 1) {
			ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
			taskExecutor.setMaxPoolSize(threads);
			taskExecutor.setCorePoolSize(threads);
			taskExecutor.afterPropertiesSet();
			stepBuilder.taskExecutor(taskExecutor).throttleLimit(threads);
		}
		Job job = factory.job(name + "-job").start(stepBuilder.build()).build();
		try {
			jobExecution = factory.runAsync(job, new JobParameters());
		} catch (Exception e) {
			throw new ItemStreamException("Could not run job " + job.getName());
		}
		try {
			jobExecution.awaitRunning();
		} catch (Exception e) {
			throw new ItemStreamException("Could not start queue job", e);
		}
		super.open(executionContext);
		log.debug("Opened {}", name);
	}

	protected FaultTolerantStepBuilder<K, K> faultTolerantStepBuilder(SimpleStepBuilder<K, K> stepBuilder) {
		return stepBuilder.faultTolerant();
	}

	@Override
	public T read() throws Exception {
		T item;
		do {
			item = queue.poll(pollTimeout, TimeUnit.MILLISECONDS);
		} while (item == null && jobExecution.isRunning());
		return item;
	}

	public List<T> read(int maxElements) {
		List<T> items = new ArrayList<>(maxElements);
		queue.drainTo(items, maxElements);
		return items;
	}

	@Override
	public synchronized void close() {
		if (jobExecution == null) {
			log.debug("Already closed, skipping");
			return;
		}
		log.debug("Closing {}", name);
		super.close();
		if (!queue.isEmpty()) {
			log.warn("Closing {} with {} items still in queue", ClassUtils.getShortName(getClass()), queue.size());
		}
		try {
			jobExecution.awaitTermination();
		} catch (Exception e) {
			throw new ItemStreamException("Error while waiting for job to terminate", e);
		}
		queue = null;
		jobExecution = null;
		log.debug("Closed {}", name);
	}

	private class ValueWriter extends AbstractItemStreamItemWriter<K> {

		@Override
		public void open(ExecutionContext executionContext) {
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
			if (valueReader instanceof ItemStream) {
				((ItemStream) valueReader).close();
			}
			super.close();
		}

		@Override
		public void write(List<? extends K> items) throws Exception {
			List<T> values = valueReader.process(items);
			if (values == null) {
				return;
			}
			for (T value : values) {
				queue.removeIf(v -> v.getKey().equals(value.getKey()));
				queue.put(value);
			}
		}

	}

	public static ScanRedisItemReaderBuilder<DataStructure<String>, DataStructureValueReader<String, String>> dataStructure(
			AbstractRedisClient client) {
		return new ScanRedisItemReaderBuilder<DataStructure<String>, DataStructureValueReader<String, String>>(client,
				new DataStructureValueReaderFactory<String, String>());
	}

	public static ScanRedisItemReaderBuilder<KeyValue<String, byte[]>, KeyDumpValueReader<String, String>> keyDump(
			AbstractRedisClient client) {
		return new ScanRedisItemReaderBuilder<KeyValue<String, byte[]>, KeyDumpValueReader<String, String>>(client,
				new KeyDumpValueReaderFactory<String, String>());
	}

	public static OffsetStreamItemReaderBuilder scan(StreamOffset<String> offset) {
		return new OffsetStreamItemReaderBuilder(offset);
	}

}
