package com.redis.spring.batch.support;

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

import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.RedisItemReader.ItemReaderBuilder;
import com.redis.spring.batch.builder.JobRepositoryBuilder;
import com.redis.spring.batch.support.compare.KeyComparisonItemWriter;
import com.redis.spring.batch.support.compare.KeyComparisonListener;
import com.redis.spring.batch.support.compare.KeyComparisonResults;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisClient;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.codec.StringCodec;

public class KeyComparator implements Callable<KeyComparisonResults> {

	private static final String NAME = "comparator";

	private final String id;
	private final JobRepository jobRepository;
	private final PlatformTransactionManager transactionManager;
	private final int chunkSize;
	private final RedisItemReader<String, DataStructure<String>> left;
	private final RedisItemReader<String, DataStructure<String>> right;
	private List<KeyComparisonListener<String>> listeners = new ArrayList<>();

	public KeyComparator(String id, JobRepository jobRepository, PlatformTransactionManager transactionManager,
			RedisItemReader<String, DataStructure<String>> left, RedisItemReader<String, DataStructure<String>> right,
			int chunkSize) {
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

	public void addListener(KeyComparisonListener<String> listener) {
		Assert.notNull(listener, "Listener cannot be null");
		this.listeners.add(listener);
	}

	public void setListeners(List<KeyComparisonListener<String>> listeners) {
		Assert.notNull(listeners, "Listener list cannot be null");
		Assert.noNullElements(listeners, "Listener list cannot contain null elements");
		this.listeners = listeners;
	}

	@Override
	public KeyComparisonResults call() throws JobExecutionException {
		String name = id + "-" + NAME;
		KeyComparisonItemWriter<String> writer = KeyComparisonItemWriter.valueReader(right.getValueReader()).build();
		writer.setListeners(listeners);
		left.setName(name + "-left-reader");
		JobRunner jobRunner = new JobRunner(jobRepository, transactionManager);
		TaskletStep step = jobRunner.step(name).<DataStructure<String>, DataStructure<String>>chunk(chunkSize)
				.reader(left).writer(writer).build();
		Job job = jobRunner.job(name).start(step).build();
		jobRunner.run(job);
		return writer.getResults();
	}

	public static RightComparatorBuilder left(RedisClient client) {
		return new RightComparatorBuilder(client);
	}

	public static RightComparatorBuilder left(RedisClusterClient client) {
		return new RightComparatorBuilder(client);
	}

	public static class RightComparatorBuilder {

		private final AbstractRedisClient left;

		public RightComparatorBuilder(AbstractRedisClient left) {
			this.left = left;
		}

		public KeyComparatorBuilder right(RedisClient client) {
			return new KeyComparatorBuilder(left, client);
		}

		public KeyComparatorBuilder right(RedisClusterClient client) {
			return new KeyComparatorBuilder(left, client);
		}

	}

	public static class KeyComparatorBuilder extends JobRepositoryBuilder<String, String, KeyComparatorBuilder> {

		public static final int DEFAULT_CHUNK_SIZE = 50;

		private String id = UUID.randomUUID().toString();
		private final AbstractRedisClient rightClient;
		private int chunkSize = DEFAULT_CHUNK_SIZE;

		public KeyComparatorBuilder(AbstractRedisClient left, AbstractRedisClient right) {
			super(left, StringCodec.UTF8);
			this.rightClient = right;
		}

		public KeyComparatorBuilder id(String id) {
			this.id = id;
			return this;
		}

		public KeyComparator build() {
			RedisItemReader<String, DataStructure<String>> left = reader(client);
			left.setName(id + "-left-reader");
			RedisItemReader<String, DataStructure<String>> right = reader(rightClient);
			right.setName(id + "-right-reader");
			return new KeyComparator(id, jobRepository, transactionManager, left, right, chunkSize);
		}

		private RedisItemReader<String, DataStructure<String>> reader(AbstractRedisClient client) {
			return readerBuilder(client).dataStructure().chunkSize(chunkSize).jobRepository(jobRepository)
					.transactionManager(transactionManager).build();
		}

		private ItemReaderBuilder readerBuilder(AbstractRedisClient client) {
			if (client instanceof RedisClusterClient) {
				return RedisItemReader.client((RedisClusterClient) client);
			}
			return RedisItemReader.client((RedisClient) client);
		}

	}

}
