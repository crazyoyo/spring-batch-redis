package com.redis.spring.batch;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.awaitility.Awaitility;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionException;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemStream;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.util.ClassUtils;

import com.redis.lettucemod.RedisModulesClient;
import com.redis.lettucemod.cluster.RedisModulesClusterClient;
import com.redis.spring.batch.common.DataStructure;
import com.redis.spring.batch.common.JobRunner;
import com.redis.spring.batch.common.KeyDump;
import com.redis.spring.batch.common.PoolOptions;
import com.redis.spring.batch.common.StepOptions;
import com.redis.spring.batch.reader.DataStructureReadOperation;
import com.redis.spring.batch.reader.KeyComparison;
import com.redis.spring.batch.reader.KeyComparisonReadOperation;
import com.redis.spring.batch.reader.KeyDumpReadOperation;
import com.redis.spring.batch.reader.LiveRedisItemReader;
import com.redis.spring.batch.reader.PollableItemReader;
import com.redis.spring.batch.reader.QueueItemWriter;
import com.redis.spring.batch.reader.QueueOptions;
import com.redis.spring.batch.reader.ReadOperation;
import com.redis.spring.batch.reader.ScanKeyItemReader;
import com.redis.spring.batch.reader.ScanOptions;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;

public class RedisItemReader<K, T> extends AbstractItemCountingItemStreamItemReader<T>
		implements PollableItemReader<T> {

	private static final Log log = LogFactory.getLog(RedisItemReader.class);

	protected final JobRunner jobRunner;
	protected final ItemReader<K> keyReader;
	private final ItemProcessor<K, K> keyProcessor;
	private final QueueItemWriter<K, T> writer;
	private final StepOptions stepOptions;
	private String name;
	private JobExecution jobExecution;

	@Override
	public void setName(String name) {
		super.setName(name);
		this.name = name;
	}

	@Override
	protected synchronized void doOpen() throws JobExecutionException {
		if (jobExecution == null) {
			SimpleStepBuilder<K, K> step = jobRunner.step(name, keyReader, keyProcessor, writer, stepOptions);
			Job job = jobRunner.job(name).start(step.build()).build();
			jobExecution = jobRunner.getAsyncJobLauncher().run(job, new JobParameters());
			jobRunner.awaitRunning(jobExecution);
			if (jobExecution.getStatus().isUnsuccessful()) {
				throw new JobExecutionException("Job execution unsuccessful");
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
	public T poll(long timeout, TimeUnit unit) throws InterruptedException {
		return writer.poll(timeout, unit);
	}

	@Override
	protected T doRead() throws InterruptedException {
		T item;
		do {
			item = writer.poll();
		} while (item == null && isOpen());
		return item;
	}

	public List<T> read(int maxElements) {
		List<T> items = new ArrayList<>(maxElements);
		writer.drainTo(items, maxElements);
		return items;
	}

	@Override
	public boolean isOpen() {
		return jobExecution != null && jobExecution.isRunning() && !jobExecution.getStatus().isUnsuccessful();
	}

	public abstract static class AbstractBuilder<K, V, B extends AbstractBuilder<K, V, B>> {

		protected final AbstractRedisClient client;
		protected final RedisCodec<K, V> codec;
		private JobRunner jobRunner;
		protected PoolOptions poolOptions = PoolOptions.builder().build();
		private QueueOptions queueOptions = QueueOptions.builder().build();
		protected StepOptions stepOptions = StepOptions.builder().build();

		protected AbstractBuilder(AbstractRedisClient client, RedisCodec<K, V> codec) {
			this.client = client;
			this.codec = codec;
		}

		@SuppressWarnings("unchecked")
		public B poolOptions(PoolOptions options) {
			this.poolOptions = options;
			return (B) this;
		}

		@SuppressWarnings("unchecked")
		public B queueOptions(QueueOptions options) {
			this.queueOptions = options;
			return (B) this;
		}

		@SuppressWarnings("unchecked")
		public B stepOptions(StepOptions options) {
			this.stepOptions = options;
			return (B) this;
		}

		@SuppressWarnings("unchecked")
		public B jobRunner(JobRunner jobRunner) {
			this.jobRunner = jobRunner;
			return (B) this;
		}

		protected final JobRunner jobRunner() {
			if (jobRunner == null) {
				return JobRunner.getInMemoryInstance();
			}
			return jobRunner;
		}

		protected <K1, V1, B1 extends AbstractBuilder<K1, V1, B1>> B1 toBuilder(B1 builder) {
			builder.jobRunner(jobRunner);
			builder.stepOptions(stepOptions);
			builder.poolOptions(poolOptions);
			builder.queueOptions(queueOptions);
			return builder;
		}

		protected DataStructureReadOperation<K, V> dataStructureValueReader() {
			return new DataStructureReadOperation<>(client, codec, poolOptions);
		}

		protected KeyDumpReadOperation<K, V> keyDumpValueReader() {
			return new KeyDumpReadOperation<>(client, codec, poolOptions);
		}

		protected <T> QueueItemWriter<K, T> queueWriter(ReadOperation<K, T> readOperation) {
			return new QueueItemWriter<>(readOperation, queueOptions);
		}

	}

	public static final Duration DEFAULT_RUNNING_TIMEOUT = Duration.ofSeconds(5);
	public static final Duration DEFAULT_FLUSHING_INTERVAL = Duration.ofMillis(50);

	protected RedisItemReader(JobRunner jobRunner, ItemReader<K> keyReader, ItemProcessor<K, K> keyProcessor,
			QueueItemWriter<K, T> queueWriter, StepOptions stepOptions) {
		setName(ClassUtils.getShortName(getClass()));
		this.jobRunner = jobRunner;
		this.keyReader = keyReader;
		this.keyProcessor = keyProcessor;
		this.writer = queueWriter;
		this.stepOptions = stepOptions;
	}

	public RedisItemReader(JobRunner jobRunner, ItemReader<K> keyReader, QueueItemWriter<K, T> queueWriter,
			StepOptions stepOptions) {
		this(jobRunner, keyReader, null, queueWriter, stepOptions);
	}

	public static Builder<String, String> client(RedisModulesClusterClient client) {
		return new Builder<>(client, StringCodec.UTF8);
	}

	public static Builder<String, String> client(RedisModulesClient client) {
		return new Builder<>(client, StringCodec.UTF8);
	}

	public static <K, V> Builder<K, V> client(RedisModulesClient client, RedisCodec<K, V> codec) {
		return new Builder<>(client, codec);
	}

	public static <K, V> Builder<K, V> client(RedisModulesClusterClient client, RedisCodec<K, V> codec) {
		return new Builder<>(client, codec);
	}

	public abstract static class AbstractReaderBuilder<K, V, B extends AbstractReaderBuilder<K, V, B>>
			extends AbstractBuilder<K, V, B> {

		protected AbstractReaderBuilder(AbstractRedisClient client, RedisCodec<K, V> codec) {
			super(client, codec);
		}

		public RedisItemReader<K, DataStructure<K>> dataStructure() {
			return reader(dataStructureValueReader());
		}

		public RedisItemReader<K, KeyDump<K>> keyDump() {
			return reader(keyDumpValueReader());
		}

		private <T> RedisItemReader<K, T> reader(ReadOperation<K, T> readOperation) {
			return reader(queueWriter(readOperation));
		}

		protected abstract <T> RedisItemReader<K, T> reader(QueueItemWriter<K, T> queueWriter);

	}

	public static class Builder<K, V> extends AbstractReaderBuilder<K, V, Builder<K, V>> {

		private ScanOptions scanOptions = ScanOptions.builder().build();

		protected Builder(AbstractRedisClient client, RedisCodec<K, V> codec) {
			super(client, codec);
		}

		public Builder<K, V> scanOptions(ScanOptions options) {
			this.scanOptions = options;
			return this;
		}

		public LiveRedisItemReader.Builder<K, V> live() {
			return toBuilder(new LiveRedisItemReader.Builder<>(client, codec)).keyPatterns(scanOptions.getMatch());
		}

		public KeyComparatorBuilder comparator(AbstractRedisClient right) {
			return toBuilder(new KeyComparatorBuilder(client, right)).leftPoolOptions(poolOptions)
					.rightPoolOptions(poolOptions).scanOptions(scanOptions);
		}

		@Override
		protected <T> RedisItemReader<K, T> reader(QueueItemWriter<K, T> queueWriter) {
			ScanKeyItemReader<K, V> keyReader = new ScanKeyItemReader<>(client, codec, scanOptions);
			return new RedisItemReader<>(super.jobRunner(), keyReader, queueWriter, stepOptions);
		}

	}

	public static class KeyComparatorBuilder extends AbstractBuilder<String, String, KeyComparatorBuilder> {

		public static final Duration DEFAULT_TTL_TOLERANCE = Duration.ofMillis(100);

		private final AbstractRedisClient right;
		private PoolOptions leftPoolOptions = PoolOptions.builder().build();
		private PoolOptions rightPoolOptions = PoolOptions.builder().build();
		private Duration ttlTolerance = DEFAULT_TTL_TOLERANCE;
		private ScanOptions scanOptions = ScanOptions.builder().build();

		protected KeyComparatorBuilder(AbstractRedisClient left, AbstractRedisClient right) {
			super(left, StringCodec.UTF8);
			this.right = right;
		}

		public KeyComparatorBuilder ttlTolerance(Duration ttlTolerance) {
			this.ttlTolerance = ttlTolerance;
			return this;
		}

		public KeyComparatorBuilder leftPoolOptions(PoolOptions options) {
			this.leftPoolOptions = options;
			return this;
		}

		public KeyComparatorBuilder rightPoolOptions(PoolOptions options) {
			this.rightPoolOptions = options;
			return this;
		}

		public KeyComparatorBuilder scanOptions(ScanOptions options) {
			this.scanOptions = options;
			return this;
		}

		public RedisItemReader<String, KeyComparison> build() {
			ScanKeyItemReader<String, String> keyReader = new ScanKeyItemReader<>(client, StringCodec.UTF8,
					scanOptions);
			return new RedisItemReader<>(super.jobRunner(), keyReader, queueWriter(readOperation()), stepOptions);
		}

		private KeyComparisonReadOperation readOperation() {
			return new KeyComparisonReadOperation(client, right, leftPoolOptions, rightPoolOptions, ttlTolerance);
		}

	}

}
