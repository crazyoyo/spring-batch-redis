package com.redis.spring.batch.support.generator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;

import org.apache.commons.lang3.Range;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.job.flow.support.SimpleFlow;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.util.Assert;

import com.redis.spring.batch.RedisItemWriter;
import com.redis.spring.batch.builder.JobRepositoryBuilder;
import com.redis.spring.batch.support.DataStructure;
import com.redis.spring.batch.support.DataStructure.Type;
import com.redis.spring.batch.support.JobRunner;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisClient;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.codec.StringCodec;
import lombok.Setter;
import lombok.experimental.Accessors;

@Setter
public class Generator implements Callable<JobExecution> {

	private static final String NAME = "generator";

	public static final int DEFAULT_CHUNK_SIZE = 50;
	public static final Range<Long> DEFAULT_SEQUENCE = Range.between(0L, 100L);
	public static final Range<Long> DEFAULT_COLLECTION_CARDINALITY = Range.is(10L);
	public static final Range<Integer> DEFAULT_STRING_VALUE_SIZE = Range.is(100);
	public static final Range<Double> DEFAULT_ZSET_SCORE = Range.between(0D, 100D);

	private final String id;
	private final JobRepository jobRepository;
	private final PlatformTransactionManager transactionManager;
	private final AbstractRedisClient client;

	private int chunkSize = DEFAULT_CHUNK_SIZE;
	private List<Type> dataTypes;
	private Range<Long> sequence = DEFAULT_SEQUENCE;
	private String keyPrefix;
	private Range<Long> expiration;
	private Range<Long> collectionCardinality = DEFAULT_COLLECTION_CARDINALITY;
	private Range<Integer> stringValueSize = DEFAULT_STRING_VALUE_SIZE;
	private Range<Double> zsetScore = DEFAULT_ZSET_SCORE;

	public Generator(AbstractRedisClient client, String id, JobRepository jobRepository,
			PlatformTransactionManager transactionManager) {
		Assert.notNull(client, "A Redis client is required");
		Assert.notNull(id, "A generator ID is required");
		Assert.notNull(jobRepository, "A job repository is required");
		Assert.notNull(transactionManager, "A platform transaction manager is required");
		this.client = client;
		this.id = id;
		this.jobRepository = jobRepository;
		this.transactionManager = transactionManager;
	}

	@Override
	public JobExecution call() throws JobExecutionAlreadyRunningException, JobRestartException,
			JobInstanceAlreadyCompleteException, JobParametersInvalidException {
		JobRunner helper = new JobRunner(jobRepository, transactionManager);
		String name = id + "-" + NAME;
		Type[] types = dataTypes();
		SimpleFlow[] subFlows = new SimpleFlow[types.length];
		for (int index = 0; index < types.length; index++) {
			Type type = types[index];
			String flowName = type + "-" + name;
			subFlows[index] = helper.flow(flowName)
					.start(chunk(helper.step(flowName)).reader(reader(type)).writer(writer()).build()).build();
		}
		SimpleAsyncTaskExecutor taskExecutor = new SimpleAsyncTaskExecutor();
		SimpleFlow flow = helper.flow(name).split(taskExecutor).add(subFlows).build();
		Job job = helper.job(name).start(flow).build().build();
		return helper.run(job);
	}

	private SimpleStepBuilder<DataStructure<String>, DataStructure<String>> chunk(StepBuilder step) {
		return step.chunk(chunkSize);
	}

	private Type[] dataTypes() {
		if (dataTypes == null || dataTypes.isEmpty()) {
			return Type.values();
		}
		return dataTypes.toArray(new Type[0]);
	}

	private ItemWriter<DataStructure<String>> writer() {
		if (client instanceof RedisClusterClient) {
			return RedisItemWriter.client((RedisClusterClient) client).dataStructure().xaddArgs(m -> null).build();
		}
		return RedisItemWriter.client((RedisClient) client).dataStructure().xaddArgs(m -> null).build();
	}

	private ItemReader<DataStructure<String>> reader(Type type) {
		switch (type) {
		case HASH:
			HashGeneratorItemReader hashReader = new HashGeneratorItemReader();
			configureDataStructure(hashReader);
			return hashReader;
		case LIST:
			ListGeneratorItemReader listReader = new ListGeneratorItemReader();
			configureCollection(listReader);
			return listReader;
		case SET:
			SetGeneratorItemReader setReader = new SetGeneratorItemReader();
			configureCollection(setReader);
			return setReader;
		case STREAM:
			StreamGeneratorItemReader streamReader = new StreamGeneratorItemReader();
			configureCollection(streamReader);
			return streamReader;
		case STRING:
			StringGeneratorItemReader stringReader = new StringGeneratorItemReader();
			stringReader.setValueSize(stringValueSize);
			configureDataStructure(stringReader);
			return stringReader;
		case ZSET:
			ZsetGeneratorItemReader zsetReader = new ZsetGeneratorItemReader();
			zsetReader.setScore(zsetScore);
			configureCollection(zsetReader);
			return zsetReader;
		}
		throw new UnsupportedOperationException(String.format("Data type '%s' is not supported", type));
	}

	private void configureCollection(CollectionGeneratorItemReader<?> reader) {
		reader.setCardinality(collectionCardinality);
		configureDataStructure(reader);
	}

	private void configureDataStructure(DataStructureGeneratorItemReader<?> reader) {
		reader.setSequence(sequence);
		reader.setKeyPrefix(keyPrefix);
		reader.setExpiration(expiration);
	}

	public static GeneratorBuilder builder(RedisClient client, String id) {
		return new GeneratorBuilder(client, id);
	}

	public static GeneratorBuilder builder(RedisClusterClient client, String id) {
		return new GeneratorBuilder(client, id);
	}

	@Accessors(fluent = true)
	public static class GeneratorBuilder extends JobRepositoryBuilder<String, String, GeneratorBuilder> {

		private final String id;

		@Setter
		private int chunkSize = DEFAULT_CHUNK_SIZE;
		private List<Type> dataTypes = new ArrayList<>();
		@Setter
		private Range<Long> sequence = DEFAULT_SEQUENCE;
		@Setter
		private String keyPrefix;
		@Setter
		private Range<Long> expiration;
		@Setter
		private Range<Long> collectionCardinality = DEFAULT_COLLECTION_CARDINALITY;
		@Setter
		private Range<Integer> stringValueSize = DEFAULT_STRING_VALUE_SIZE;
		@Setter
		private Range<Double> zsetScore = DEFAULT_ZSET_SCORE;

		public GeneratorBuilder(AbstractRedisClient client, String id) {
			super(client, StringCodec.UTF8);
			this.id = id;
		}

		public GeneratorBuilder dataType(Type type) {
			this.dataTypes.add(type);
			return this;
		}

		public GeneratorBuilder dataTypes(Type... types) {
			this.dataTypes = new ArrayList<>(Arrays.asList(types));
			return this;
		}

		public GeneratorBuilder end(long end) {
			sequence(Range.between(0L, end));
			return this;
		}

		public GeneratorBuilder between(long start, long end) {
			sequence(Range.between(start, end));
			return this;
		}

		public Generator build() {
			Generator generator = new Generator(client, id, jobRepository, transactionManager);
			generator.setChunkSize(chunkSize);
			generator.setCollectionCardinality(collectionCardinality);
			generator.setDataTypes(dataTypes);
			generator.setExpiration(expiration);
			generator.setKeyPrefix(keyPrefix);
			generator.setSequence(sequence);
			generator.setStringValueSize(stringValueSize);
			generator.setZsetScore(zsetScore);
			return generator;
		}
	}

}
