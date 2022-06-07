package com.redis.spring.batch.compare;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Callable;

import org.springframework.batch.core.JobExecutionException;
import org.springframework.util.Assert;

import com.redis.spring.batch.DataStructure;
import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.RedisItemReader.Builder;
import com.redis.spring.batch.support.JobRunner;
import com.redis.spring.batch.support.RedisConnectionBuilder;
import com.redis.spring.batch.support.Utils;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.codec.StringCodec;

public class KeyComparator implements Callable<KeyComparisonResults> {

	private static final String NAME = "comparator";

	private final String id;
	private final JobRunner jobRunner;
	private final int chunkSize;
	private final RedisItemReader<String, DataStructure<String>> left;
	private final RedisItemReader<String, DataStructure<String>> right;
	private List<KeyComparisonListener> listeners = new ArrayList<>();

	public KeyComparator(String id, JobRunner jobRunner, RedisItemReader<String, DataStructure<String>> left,
			RedisItemReader<String, DataStructure<String>> right, int chunkSize) {
		Assert.notNull(id, "An ID is required");
		Assert.notNull(jobRunner, "A job runner is required");
		Assert.notNull(left, "Left Redis client is required");
		Assert.notNull(right, "Right Redis client is required");
		Utils.assertPositive(chunkSize, "Chunk size");
		this.id = id;
		this.jobRunner = jobRunner;
		this.left = left;
		this.right = right;
		this.chunkSize = chunkSize;
	}

	public void addListener(KeyComparisonListener listener) {
		Assert.notNull(listener, "Listener cannot be null");
		this.listeners.add(listener);
	}

	public void setListeners(List<KeyComparisonListener> listeners) {
		Assert.notNull(listeners, "Listener list cannot be null");
		Assert.noNullElements(listeners, "Listener list cannot contain null elements");
		this.listeners = listeners;
	}

	@Override
	public KeyComparisonResults call() throws JobExecutionException {
		String name = id + "-" + NAME;
		KeyComparisonItemWriter writer = KeyComparisonItemWriter.valueReader(right.getValueReader()).build();
		writer.setListeners(listeners);
		left.setName(name + "-left-reader");
		jobRunner.run(name, chunkSize, left, writer);
		return writer.getResults();
	}

	public static RightComparatorBuilder left(AbstractRedisClient client) {
		return new RightComparatorBuilder(client);
	}

	public static class RightComparatorBuilder {

		private final AbstractRedisClient left;

		public RightComparatorBuilder(AbstractRedisClient left) {
			this.left = left;
		}

		public KeyComparatorBuilder right(AbstractRedisClient client) {
			return new KeyComparatorBuilder(left, client);
		}

	}

	public static class KeyComparatorBuilder extends RedisConnectionBuilder<String, String, KeyComparatorBuilder> {

		public static final int DEFAULT_CHUNK_SIZE = 50;

		private String id = UUID.randomUUID().toString();
		private final AbstractRedisClient rightClient;
		private int chunkSize = DEFAULT_CHUNK_SIZE;
		private Optional<JobRunner> jobRunner = Optional.empty();

		public KeyComparatorBuilder(AbstractRedisClient left, AbstractRedisClient right) {
			super(left, StringCodec.UTF8);
			this.rightClient = right;
		}

		public KeyComparatorBuilder id(String id) {
			this.id = id;
			return this;
		}

		public KeyComparatorBuilder jobRunner(JobRunner jobRunner) {
			this.jobRunner = Optional.of(jobRunner);
			return this;
		}

		public KeyComparator build() throws Exception {
			RedisItemReader<String, DataStructure<String>> left = reader(client);
			left.setName(id + "-left-reader");
			RedisItemReader<String, DataStructure<String>> right = reader(rightClient);
			right.setName(id + "-right-reader");
			return new KeyComparator(id, jobRunner.isEmpty() ? JobRunner.inMemory() : jobRunner.get(), left, right,
					chunkSize);
		}

		private RedisItemReader<String, DataStructure<String>> reader(AbstractRedisClient client) throws Exception {
			return readerBuilder(client).dataStructure().chunkSize(chunkSize).jobRunner(jobRunner).build();
		}

		private Builder<String, String> readerBuilder(AbstractRedisClient client) {
			return RedisItemReader.client(client, codec);
		}

	}

}
