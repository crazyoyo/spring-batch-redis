package com.redis.spring.batch.compare;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecutionException;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.util.Assert;

import com.redis.spring.batch.DataStructure;
import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.RedisItemReader.Builder;
import com.redis.spring.batch.support.JobRepositoryBuilder;
import com.redis.spring.batch.support.JobRunner;
import com.redis.spring.batch.support.Utils;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisClient;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.codec.RedisCodec;

public class KeyComparator<K, V> implements Callable<KeyComparisonResults> {

	private static final String NAME = "comparator";

	private final String id;
	private final JobRepository jobRepository;
	private final PlatformTransactionManager transactionManager;
	private final int chunkSize;
	private final RedisItemReader<K, DataStructure<K>> left;
	private final RedisItemReader<K, DataStructure<K>> right;
	private List<KeyComparisonListener<K>> listeners = new ArrayList<>();

	public KeyComparator(String id, JobRepository jobRepository, PlatformTransactionManager transactionManager,
			RedisItemReader<K, DataStructure<K>> left, RedisItemReader<K, DataStructure<K>> right, int chunkSize) {
		Assert.notNull(id, "An ID is required");
		Assert.notNull(jobRepository, "A job repository is required");
		Assert.notNull(transactionManager, "A platform transaction manager is required");
		Assert.notNull(left, "Left Redis client is required");
		Assert.notNull(right, "Right Redis client is required");
		Utils.assertPositive(chunkSize, "Chunk size");
		this.id = id;
		this.jobRepository = jobRepository;
		this.transactionManager = transactionManager;
		this.left = left;
		this.right = right;
		this.chunkSize = chunkSize;
	}

	public void addListener(KeyComparisonListener<K> listener) {
		Assert.notNull(listener, "Listener cannot be null");
		this.listeners.add(listener);
	}

	public void setListeners(List<KeyComparisonListener<K>> listeners) {
		Assert.notNull(listeners, "Listener list cannot be null");
		Assert.noNullElements(listeners, "Listener list cannot contain null elements");
		this.listeners = listeners;
	}

	@Override
	public KeyComparisonResults call() throws JobExecutionException {
		String name = id + "-" + NAME;
		KeyComparisonItemWriter<K> writer = KeyComparisonItemWriter.valueReader(right.getValueReader()).build();
		writer.setListeners(listeners);
		left.setName(name + "-left-reader");
		JobRunner jobRunner = new JobRunner(jobRepository, transactionManager);
		TaskletStep step = jobRunner.step(name).<DataStructure<K>, DataStructure<K>>chunk(chunkSize).reader(left)
				.writer(writer).build();
		Job job = jobRunner.job(name).start(step).build();
		jobRunner.run(job);
		return writer.getResults();
	}

	public static <K, V> RightComparatorBuilder<K, V> left(RedisClient client, RedisCodec<K, V> codec) {
		return new RightComparatorBuilder<>(client, codec);
	}

	public static <K, V> RightComparatorBuilder<K, V> left(RedisClusterClient client, RedisCodec<K, V> codec) {
		return new RightComparatorBuilder<>(client, codec);
	}

	public static class RightComparatorBuilder<K, V> {

		private final AbstractRedisClient left;
		private final RedisCodec<K, V> codec;

		public RightComparatorBuilder(AbstractRedisClient left, RedisCodec<K, V> codec) {
			this.left = left;
			this.codec = codec;
		}

		public KeyComparatorBuilder<K, V> right(RedisClient client) {
			return new KeyComparatorBuilder<>(left, codec, client);
		}

		public KeyComparatorBuilder<K, V> right(RedisClusterClient client) {
			return new KeyComparatorBuilder<>(left, codec, client);
		}

	}

	public static class KeyComparatorBuilder<K, V> extends JobRepositoryBuilder<K, V, KeyComparatorBuilder<K, V>> {

		public static final int DEFAULT_CHUNK_SIZE = 50;

		private String id = UUID.randomUUID().toString();
		private final AbstractRedisClient rightClient;
		private int chunkSize = DEFAULT_CHUNK_SIZE;

		public KeyComparatorBuilder(AbstractRedisClient left, RedisCodec<K, V> codec, AbstractRedisClient right) {
			super(left, codec);
			this.rightClient = right;
		}

		public KeyComparatorBuilder<K, V> id(String id) {
			this.id = id;
			return this;
		}

		public KeyComparator<K, V> build() {
			RedisItemReader<K, DataStructure<K>> left = reader(client);
			left.setName(id + "-left-reader");
			RedisItemReader<K, DataStructure<K>> right = reader(rightClient);
			right.setName(id + "-right-reader");
			return new KeyComparator<>(id, jobRepository, transactionManager, left, right, chunkSize);
		}

		private RedisItemReader<K, DataStructure<K>> reader(AbstractRedisClient client) {
			return readerBuilder(client).dataStructure().chunkSize(chunkSize).jobRepository(jobRepository)
					.transactionManager(transactionManager).build();
		}

		private Builder<K, V> readerBuilder(AbstractRedisClient client) {
			if (client instanceof RedisClusterClient) {
				return RedisItemReader.client((RedisClusterClient) client, codec);
			}
			return RedisItemReader.client((RedisClient) client, codec);
		}

	}

}
