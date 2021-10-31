package com.redis.spring.batch.support;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.core.task.SyncTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.util.Assert;

import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.support.compare.KeyComparisonItemWriter;
import com.redis.spring.batch.support.compare.KeyComparisonListener;
import com.redis.spring.batch.support.compare.KeyComparisonResults;

import io.lettuce.core.AbstractRedisClient;

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
	public KeyComparisonResults call() throws JobExecutionAlreadyRunningException, JobRestartException,
			JobInstanceAlreadyCompleteException, JobParametersInvalidException {
		String name = id + "-" + NAME;
		StepBuilderFactory stepBuilderFactory = new StepBuilderFactory(jobRepository, transactionManager);
		KeyComparisonItemWriter<String> writer = KeyComparisonItemWriter.valueReader(right.getValueReader()).build();
		writer.setListeners(listeners);
		left.setName(name + "-left-reader");
		TaskletStep step = stepBuilderFactory.get(name).<DataStructure<String>, DataStructure<String>>chunk(chunkSize)
				.reader(left).writer(writer).build();
		Job job = new JobBuilderFactory(jobRepository).get(name).start(step).build();
		SimpleJobLauncher launcher = new SimpleJobLauncher();
		launcher.setJobRepository(jobRepository);
		launcher.setTaskExecutor(new SyncTaskExecutor());
		launcher.run(job, new JobParameters());
		return writer.getResults();
	}

	public static LeftKeyComparatorBuilder builder(String id, JobRepository jobRepository,
			PlatformTransactionManager transactionManager) {
		return new LeftKeyComparatorBuilder(id, jobRepository, transactionManager);
	}

	public static class LeftKeyComparatorBuilder {

		private final String id;
		private final JobRepository jobRepository;
		private final PlatformTransactionManager transactionManager;

		public LeftKeyComparatorBuilder(String id, JobRepository jobRepository,
				PlatformTransactionManager transactionManager) {
			this.id = id;
			this.jobRepository = jobRepository;
			this.transactionManager = transactionManager;
		}

		public RightKeyComparatorBuilder left(AbstractRedisClient client) {
			RedisItemReader<String, DataStructure<String>> left = RedisItemReader
					.dataStructure(jobRepository, transactionManager, client).build();
			return new RightKeyComparatorBuilder(id, jobRepository, transactionManager, left);
		}

	}

	public static class RightKeyComparatorBuilder {

		private final String id;
		private final JobRepository jobRepository;
		private final PlatformTransactionManager transactionManager;
		private final RedisItemReader<String, DataStructure<String>> left;

		public RightKeyComparatorBuilder(String id, JobRepository jobRepository,
				PlatformTransactionManager transactionManager, RedisItemReader<String, DataStructure<String>> left) {
			this.id = id;
			this.jobRepository = jobRepository;
			this.transactionManager = transactionManager;
			this.left = left;
		}

		public KeyComparatorBuilder right(AbstractRedisClient client) {
			RedisItemReader<String, DataStructure<String>> right = RedisItemReader
					.dataStructure(jobRepository, transactionManager, client).build();
			return new KeyComparatorBuilder(id, jobRepository, transactionManager, left, right);
		}

	}

	public static class KeyComparatorBuilder {

		public static final int DEFAULT_CHUNK_SIZE = 50;

		private final String id;
		private final JobRepository jobRepository;
		private final PlatformTransactionManager transactionManager;
		private final RedisItemReader<String, DataStructure<String>> left;
		private final RedisItemReader<String, DataStructure<String>> right;
		private int chunkSize = DEFAULT_CHUNK_SIZE;

		public KeyComparatorBuilder(String id, JobRepository jobRepository,
				PlatformTransactionManager transactionManager, RedisItemReader<String, DataStructure<String>> left,
				RedisItemReader<String, DataStructure<String>> right) {
			this.id = id;
			this.jobRepository = jobRepository;
			this.transactionManager = transactionManager;
			this.left = left;
			this.right = right;
		}

		public KeyComparator build() {
			return new KeyComparator(id, jobRepository, transactionManager, left, right, chunkSize);
		}

	}

}
