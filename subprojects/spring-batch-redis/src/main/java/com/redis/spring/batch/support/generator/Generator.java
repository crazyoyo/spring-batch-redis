package com.redis.spring.batch.support.generator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;

import org.apache.commons.lang3.Range;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionException;
import org.springframework.batch.core.job.flow.support.SimpleFlow;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemReader;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

import com.redis.spring.batch.RedisItemWriter;
import com.redis.spring.batch.builder.JobRepositoryBuilder;
import com.redis.spring.batch.support.DataStructure;
import com.redis.spring.batch.support.JobRunner;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisClient;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.codec.StringCodec;

public class Generator implements Callable<JobExecution> {

	public enum Type {
		STRING, LIST, SET, ZSET, HASH, STREAM;
	}

	private static final String NAME = "generator";

	public static final int DEFAULT_CHUNK_SIZE = 50;
	public static final Range<Long> DEFAULT_SEQUENCE = Range.between(0L, 100L);
	public static final Range<Long> DEFAULT_COLLECTION_CARDINALITY = Range.is(10L);
	public static final Range<Integer> DEFAULT_STRING_VALUE_SIZE = Range.is(100);
	public static final Range<Double> DEFAULT_ZSET_SCORE = Range.between(0D, 100D);

	private final AbstractRedisClient client;
	private final String id;
	private final JobRepository jobRepository;
	private final PlatformTransactionManager transactionManager;
	private final int chunkSize;
	private final Set<Type> types;
	private final Range<Long> sequence;
	private final Range<Long> expiration;
	private final Range<Long> collectionCardinality;
	private final Range<Integer> stringValueSize;
	private final Range<Double> zsetScore;

	private Generator(GeneratorBuilder builder) {
		this.client = builder.getClient();
		this.id = builder.id;
		this.jobRepository = builder.getJobRepository();
		this.transactionManager = builder.getTransactionManager();
		this.chunkSize = builder.chunkSize;
		this.types = builder.types;
		this.sequence = builder.sequence;
		this.expiration = builder.expiration;
		this.collectionCardinality = builder.collectionCardinality;
		this.stringValueSize = builder.stringValueSize;
		this.zsetScore = builder.zsetScore;
	}

	@Override
	public JobExecution call() throws JobExecutionException {
		JobRunner jobRunner = new JobRunner(jobRepository, transactionManager);
		String name = id + "-" + NAME;
		Set<Type> readerTypes = this.types.isEmpty() ? new LinkedHashSet<>(Arrays.asList(Type.values())) : this.types;
		List<SimpleFlow> subFlows = new ArrayList<>();
		for (Type type : readerTypes) {
			String flowName = type + "-" + name;
			subFlows.add(jobRunner.flow(flowName)
					.start(chunk(jobRunner.step(flowName)).reader(reader(type)).writer(writer()).build()).build());
		}
		SimpleAsyncTaskExecutor taskExecutor = new SimpleAsyncTaskExecutor();
		SimpleFlow flow = jobRunner.flow(name).split(taskExecutor).add(subFlows.toArray(new SimpleFlow[0])).build();
		Job job = jobRunner.job(name).start(flow).build().build();
		return jobRunner.run(job);
	}

	private SimpleStepBuilder<DataStructure<String>, DataStructure<String>> chunk(StepBuilder step) {
		return step.chunk(chunkSize);
	}

	private RedisItemWriter<String, String, DataStructure<String>> writer() {
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
		default:
			throw new UnsupportedOperationException(String.format("Data type '%s' is not supported", type));
		}
	}

	private void configureCollection(CollectionGeneratorItemReader<?> reader) {
		reader.setCardinality(collectionCardinality);
		configureDataStructure(reader);
	}

	private void configureDataStructure(DataStructureGeneratorItemReader<?> reader) {
		reader.setSequence(sequence);
		reader.setExpiration(expiration);
	}

	public static ClientGeneratorBuilder client(RedisClient client) {
		return new ClientGeneratorBuilder(client);
	}

	public static ClientGeneratorBuilder client(RedisClusterClient client) {
		return new ClientGeneratorBuilder(client);
	}

	public static class ClientGeneratorBuilder {

		private final AbstractRedisClient client;

		public ClientGeneratorBuilder(AbstractRedisClient client) {
			this.client = client;
		}

		public GeneratorBuilder id(String id) {
			return new GeneratorBuilder(client, id);
		}
	}

	public static class GeneratorBuilder extends JobRepositoryBuilder<String, String, GeneratorBuilder> {

		private final String id;

		private int chunkSize = DEFAULT_CHUNK_SIZE;
		private Set<Type> types = new LinkedHashSet<>();
		private Range<Long> sequence = DEFAULT_SEQUENCE;
		private Range<Long> expiration;
		private Range<Long> collectionCardinality = DEFAULT_COLLECTION_CARDINALITY;
		private Range<Integer> stringValueSize = DEFAULT_STRING_VALUE_SIZE;
		private Range<Double> zsetScore = DEFAULT_ZSET_SCORE;

		public GeneratorBuilder(AbstractRedisClient client, String id) {
			super(client, StringCodec.UTF8);
			this.id = id;
		}

		public GeneratorBuilder chunkSize(int chunkSize) {
			this.chunkSize = chunkSize;
			return this;
		}

		public GeneratorBuilder sequence(Range<Long> sequence) {
			this.sequence = sequence;
			return this;
		}

		public GeneratorBuilder expiration(Range<Long> expiration) {
			this.expiration = expiration;
			return this;
		}

		public GeneratorBuilder collectionCardinality(Range<Long> collectionCardinality) {
			this.collectionCardinality = collectionCardinality;
			return this;
		}

		public GeneratorBuilder stringValueSize(Range<Integer> stringValueSize) {
			this.stringValueSize = stringValueSize;
			return this;
		}

		public GeneratorBuilder zsetScore(Range<Double> zsetScore) {
			this.zsetScore = zsetScore;
			return this;
		}

		public GeneratorBuilder type(Type type) {
			this.types.add(type);
			return this;
		}

		public GeneratorBuilder types(Type... types) {
			this.types.addAll(Arrays.asList(types));
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
			return new Generator(this);
		}
	}

}
