package com.redis.spring.batch.support.generator;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

import org.apache.commons.lang3.Range;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.job.flow.support.SimpleFlow;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.SyncTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

import com.redis.spring.batch.RedisItemWriter;
import com.redis.spring.batch.support.DataStructure;
import com.redis.spring.batch.support.DataStructure.Type;
import com.redis.spring.batch.support.generator.CollectionGeneratorItemReader.CollectionOptions;
import com.redis.spring.batch.support.generator.DataStructureGeneratorItemReader.DataStructureOptions;
import com.redis.spring.batch.support.generator.StringGeneratorItemReader.StringOptions;
import com.redis.spring.batch.support.generator.ZsetGeneratorItemReader.ZsetOptions;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisClient;
import io.lettuce.core.cluster.RedisClusterClient;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Singular;

@Builder(builderMethodName = "privateBuilder")
public class Generator implements Callable<JobExecution> {

	private static final String NAME = "generator";

	public static final int DEFAULT_CHUNK_SIZE = 50;
	public static final Type[] DEFAULT_DATATYPES = Type.values();
	public static final Range<Long> DEFAULT_SEQUENCE = Range.between(0L, 100L);
	public static final Range<Long> DEFAULT_COLLECTION_CARDINALITY = Range.is(10L);
	public static final Range<Integer> DEFAULT_STRING_VALUE_SIZE = Range.is(100);
	public static final Range<Double> DEFAULT_ZSET_SCORE = Range.between(0D, 100D);

	private final String id;
	private final JobRepository jobRepository;
	private final PlatformTransactionManager transactionManager;
	private final AbstractRedisClient client;

	@Default
	private int chunkSize = DEFAULT_CHUNK_SIZE;
	@Singular
	private List<Type> dataTypes;
	@Default
	private Range<Long> sequence = DEFAULT_SEQUENCE;
	private String keyPrefix;
	private Range<Long> expiration;
	@Default
	private Range<Long> collectionCardinality = DEFAULT_COLLECTION_CARDINALITY;
	@Default
	private Range<Integer> stringValueSize = DEFAULT_STRING_VALUE_SIZE;
	@Default
	private Range<Double> zsetScore = DEFAULT_ZSET_SCORE;

	public static GeneratorBuilder builder(String id, JobRepository jobRepository,
			PlatformTransactionManager transactionManager, AbstractRedisClient client) {
		return new GeneratorBuilder().id(id).jobRepository(jobRepository).transactionManager(transactionManager)
				.client(client);
	}

	@Override
	public JobExecution call() throws JobExecutionAlreadyRunningException, JobRestartException,
			JobInstanceAlreadyCompleteException, JobParametersInvalidException {
		SimpleJobLauncher launcher = new SimpleJobLauncher();
		launcher.setJobRepository(jobRepository);
		launcher.setTaskExecutor(new SyncTaskExecutor());
		return launcher.run(job(), new JobParameters());
	}

	public Job job() {
		String name = id + "-" + NAME;
		return new JobBuilderFactory(jobRepository).get(name).start(flow(name)).build().build();
	}

	private Flow flow(String name) {
		StepBuilderFactory stepBuilderFactory = new StepBuilderFactory(jobRepository, transactionManager);
		List<SimpleFlow> subFlows = new ArrayList<>();
		for (Type type : dataTypes()) {
			subFlows.add(new FlowBuilder<SimpleFlow>(type + "-" + name).start(stepBuilderFactory.get(type + "-" + name)
					.<DataStructure<String>, DataStructure<String>>chunk(chunkSize).reader(reader(type))
					.writer(writer()).build()).build());
		}
		return new FlowBuilder<SimpleFlow>(name).split(new SimpleAsyncTaskExecutor())
				.add(subFlows.toArray(new SimpleFlow[0])).build();
	}

	private Type[] dataTypes() {
		if (dataTypes.isEmpty()) {
			return Type.values();
		}
		return dataTypes.toArray(new Type[0]);
	}

	private ItemWriter<DataStructure<String>> writer() {
		if (client instanceof RedisClusterClient) {
			return RedisItemWriter.dataStructure((RedisClusterClient) client, m -> null).build();
		}
		return RedisItemWriter.dataStructure((RedisClient) client, m -> null).build();
	}

	private ItemReader<DataStructure<String>> reader(Type type) {
		switch (type) {
		case HASH:
			return new HashGeneratorItemReader(configureDataStructure(DataStructureOptions.builder().build()));
		case LIST:
			return new ListGeneratorItemReader(configureCollection(CollectionOptions.builder().build()));
		case SET:
			return new SetGeneratorItemReader(configureCollection(CollectionOptions.builder().build()));
		case STREAM:
			return new StreamGeneratorItemReader(configureCollection(CollectionOptions.builder().build()));
		case STRING:
			return new StringGeneratorItemReader(configureString(StringOptions.builder().build()));
		case ZSET:
			return new ZsetGeneratorItemReader(configureZset(ZsetOptions.builder().build()));
		}
		throw new UnsupportedOperationException(String.format("Data type '%s' is not supported", type));
	}

	private ZsetOptions configureZset(ZsetOptions options) {
		options.setScore(zsetScore);
		configureCollection(options);
		return options;
	}

	private StringOptions configureString(StringOptions options) {
		options.setValueSize(stringValueSize);
		configureDataStructure(options);
		return options;
	}

	private CollectionOptions configureCollection(CollectionOptions options) {
		options.setCardinality(collectionCardinality);
		configureDataStructure(options);
		return options;
	}

	private DataStructureOptions configureDataStructure(DataStructureOptions options) {
		options.setSequence(sequence);
		options.setKeyPrefix(keyPrefix);
		options.setExpiration(expiration);
		return options;
	}

	public static class GeneratorBuilder {

		public GeneratorBuilder end(long end) {
			sequence(Range.between(0L, end));
			return this;
		}

		public GeneratorBuilder between(long start, long end) {
			sequence(Range.between(start, end));
			return this;
		}
	}

}
