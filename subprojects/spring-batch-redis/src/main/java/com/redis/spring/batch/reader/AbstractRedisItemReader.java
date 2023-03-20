package com.redis.spring.batch.reader;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.awaitility.Awaitility;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.job.builder.SimpleJobBuilder;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemStream;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.batch.item.support.AbstractItemStreamItemWriter;
import org.springframework.util.ClassUtils;

import com.redis.spring.batch.common.JobRunner;
import com.redis.spring.batch.common.Openable;
import com.redis.spring.batch.common.Utils;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.codec.RedisCodec;

public abstract class AbstractRedisItemReader<K, T> extends AbstractItemCountingItemStreamItemReader<T>
		implements Openable {

	private static final Log log = LogFactory.getLog(AbstractRedisItemReader.class);

	protected final JobRunner jobRunner;
	protected final ItemReader<K> keyReader;
	private final ItemProcessor<K, K> keyProcessor;
	private final Writer<K, T> writer;
	private final ReaderOptions options;
	protected final BlockingQueue<T> queue;
	private String name;
	private JobExecution jobExecution;

	protected AbstractRedisItemReader(JobRunner jobRunner, ItemReader<K> keyReader, ItemProcessor<K, K> keyProcessor,
			ValueReader<K, T> valueReader, ReaderOptions options) {
		setName(ClassUtils.getShortName(getClass()));
		this.jobRunner = jobRunner;
		this.keyReader = options.getThreads() > 1 ? JobRunner.synchronize(keyReader) : keyReader;
		this.keyProcessor = keyProcessor;
		this.queue = new LinkedBlockingQueue<>(options.getQueueOptions().getCapacity());
		this.writer = new Writer<>(valueReader, queue);
		this.options = options;
	}

	@Override
	public void setName(String name) {
		super.setName(name);
		this.name = name;
	}

	@Override
	protected synchronized void doOpen() throws Exception {
		if (jobExecution == null) {
			Utils.createGaugeCollectionSize("reader.queue.size", queue);
			jobExecution = jobRunner.getAsyncJobLauncher().run(job().build(), new JobParameters());
			jobRunner.awaitRunning(jobExecution);
			if (jobExecution.getStatus().isUnsuccessful()) {
				throw new ItemStreamException("Job execution unsuccessful");
			}
		}
	}

	protected SimpleJobBuilder job() {
		return jobRunner.job(name).start(JobRunner.faultTolerant(step(), options.getFaultToleranceOptions()).build());
	}

	protected SimpleStepBuilder<K, K> step() {
		SimpleStepBuilder<K, K> step = jobRunner.step(name).chunk(options.getChunkSize());
		step.reader(keyReader);
		step.processor(keyProcessor);
		step.writer(writer);
		JobRunner.multiThreaded(step, options.getThreads());
		return step;
	}

	private static class Writer<K, T> extends AbstractItemStreamItemWriter<K> implements Openable {

		private final ValueReader<K, T> valueReader;
		private final BlockingQueue<T> queue;
		private boolean open;

		public Writer(ValueReader<K, T> valueReader, BlockingQueue<T> queue) {
			this.valueReader = valueReader;
			this.queue = queue;
		}

		@Override
		public void open(ExecutionContext executionContext) {
			super.open(executionContext);
			if (valueReader instanceof ItemStream) {
				((ItemStream) valueReader).open(executionContext);
			}
			this.open = true;
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
			this.open = false;
		}

		@Override
		public boolean isOpen() {
			return open;
		}

		@Override
		public void write(List<? extends K> items) throws InterruptedException {
			List<T> values;
			try {
				values = valueReader.read(items);
			} catch (Exception e) {
				throw new ItemStreamException("Could not read values", e);
			}
			for (T item : values) {
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
		log.info("Closing reader " + name);
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
			item = queue.poll(options.getQueueOptions().getPollTimeout().toMillis(), TimeUnit.MILLISECONDS);
		} while (item == null && isOpen());
		return item;
	}

	@Override
	public boolean isOpen() {
		return jobExecution != null && jobExecution.isRunning() && !jobExecution.getStatus().isUnsuccessful();
	}

	public abstract static class AbstractReaderBuilder<K, V, B extends AbstractReaderBuilder<K, V, B>> {

		private static JobRunner inMemoryJobRunner;

		private JobRunner jobRunner;
		protected final AbstractRedisClient client;
		protected final RedisCodec<K, V> codec;
		protected ReaderOptions readerOptions = ReaderOptions.builder().build();

		protected AbstractReaderBuilder(AbstractRedisClient client, RedisCodec<K, V> codec) {
			this.client = client;
			this.codec = codec;
		}

		protected abstract ItemReader<K> keyReader();

		protected final JobRunner jobRunner() {
			if (jobRunner == null) {
				return inMemoryJobRunner();
			}
			return jobRunner;
		}

		private static JobRunner inMemoryJobRunner() {
			if (inMemoryJobRunner == null) {
				inMemoryJobRunner = JobRunner.inMemory();
			}
			return inMemoryJobRunner;
		}

		protected <K1, V1, B1 extends AbstractReaderBuilder<K1, V1, B1>> B1 toBuilder(B1 builder) {
			builder.jobRunner(jobRunner);
			builder.readerOptions(readerOptions);
			return builder;
		}

		@SuppressWarnings("unchecked")
		public B jobRunner(JobRunner jobRunner) {
			this.jobRunner = jobRunner;
			return (B) this;
		}

		@SuppressWarnings("unchecked")
		public B readerOptions(ReaderOptions options) {
			this.readerOptions = options;
			return (B) this;
		}

		protected DataStructureValueReader<K, V> dataStructureValueReader() {
			return new DataStructureValueReader<>(client, codec, readerOptions.getPoolOptions());
		}

		protected KeyDumpValueReader<K, V> keyDumpValueReader() {
			return new KeyDumpValueReader<>(client, codec, readerOptions.getPoolOptions());
		}

	}

}
