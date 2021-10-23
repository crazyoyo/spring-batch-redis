package com.redis.spring.batch.support.generator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;

import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.builder.FlowJobBuilder;
import org.springframework.batch.core.job.flow.support.SimpleFlow;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.core.task.SimpleAsyncTaskExecutor;

import com.redis.spring.batch.RedisItemWriter;
import com.redis.spring.batch.support.DataStructure;
import com.redis.spring.batch.support.job.JobFactory;

import io.lettuce.core.AbstractRedisClient;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Data;
import lombok.Getter;

public class Generator implements Callable<JobExecution> {

	public enum DataType {
		HASH, LIST, SET, STREAM, STRING, ZSET
	}

	private static final String NAME = "generator";

	private final String id;
	private final JobFactory jobFactory;
	private final AbstractRedisClient client;
	@Getter
	private final Options options;

	public Generator(String id, JobFactory jobFactory, AbstractRedisClient client, Options options) {
		this.id = id;
		this.jobFactory = jobFactory;
		this.client = client;
		this.options = options;
	}

	@Override
	public JobExecution call() throws Exception {
		String name = id + "-" + NAME;
		List<SimpleFlow> flows = new ArrayList<>();
		for (DataType type : options.getDataTypes()) {
			FlowBuilder<SimpleFlow> flow = jobFactory.flow(type + "-" + name);
			flows.add(flow
					.start(jobFactory.step(type + "-" + name, options.getChunkSize(), reader(type), writer()).build())
					.build());
		}
		SimpleFlow flow = jobFactory.flow(name).split(new SimpleAsyncTaskExecutor())
				.add(flows.toArray(new SimpleFlow[0])).build();
		FlowJobBuilder job = jobFactory.job(name).start(flow).build();
		JobExecution execution = jobFactory.run(job.build(), new JobParameters());
		jobFactory.awaitTermination(execution);
		return execution;
	}

	private ItemWriter<DataStructure<String>> writer() {
		return RedisItemWriter.dataStructure(client, m -> null).build();
	}

	private ItemReader<DataStructure<String>> reader(DataType type) {
		switch (type) {
		case HASH:
			return new HashGeneratorItemReader(options.getHashOptions());
		case LIST:
			return new ListGeneratorItemReader(options.getListOptions());
		case SET:
			return new SetGeneratorItemReader(options.getSetOptions());
		case STREAM:
			return new StreamGeneratorItemReader(options.getStreamOptions());
		case STRING:
			return new StringGeneratorItemReader(options.getStringOptions());
		case ZSET:
			return new ZsetGeneratorItemReader(options.getZsetOptions());
		}
		throw new UnsupportedOperationException(String.format("Data type '%s' is not supported", type));
	}

	public static JobFactoryGeneratorBuilder id(String id) {
		return new JobFactoryGeneratorBuilder(id);
	}

	public static class JobFactoryGeneratorBuilder {

		private final String id;

		public JobFactoryGeneratorBuilder(String id) {
			this.id = id;
		}

		public ClientGeneratorBuilder jobFactory(JobFactory jobFactory) {
			return new ClientGeneratorBuilder(id, jobFactory);
		}

	}

	public static class ClientGeneratorBuilder {

		private final String id;
		private final JobFactory jobFactory;

		public ClientGeneratorBuilder(String id, JobFactory jobFactory) {
			this.id = id;
			this.jobFactory = jobFactory;
		}

		public GeneratorBuilder client(AbstractRedisClient client) {
			return new GeneratorBuilder(id, jobFactory, client);
		}
	}

	public static class GeneratorBuilder {

		private final String id;
		private final JobFactory jobFactory;
		private final AbstractRedisClient client;
		private Options options = Options.builder().build();

		public GeneratorBuilder(String id, JobFactory jobFactory, AbstractRedisClient client) {
			this.id = id;
			this.jobFactory = jobFactory;
			this.client = client;
		}

		public GeneratorBuilder dataTypes(DataType... dataTypes) {
			this.options.setDataTypes(Arrays.asList(dataTypes));
			return this;
		}

		public GeneratorBuilder options(Options options) {
			this.options = options;
			return this;
		}

		public GeneratorBuilder hash(DataStructureGeneratorItemReader.Options hashOptions) {
			this.options.setHashOptions(hashOptions);
			return this;
		}

		public GeneratorBuilder string(DataStructureGeneratorItemReader.Options hashOptions) {
			this.options.setStringOptions(hashOptions);
			return this;
		}

		public Generator build() {
			return new Generator(id, jobFactory, client, options);
		}

		public GeneratorBuilder to(int max) {
			this.options.to(max);
			return this;
		}

		public GeneratorBuilder keyPrefix(String prefix) {
			this.options.keyPrefix(prefix);
			return this;
		}

	}

	@Data
	@Builder
	public static class Options {

		public static final int DEFAULT_CHUNK_SIZE = 50;

		@Default
		private List<DataType> dataTypes = Arrays.asList(DataType.values());
		@Default
		private int chunkSize = DEFAULT_CHUNK_SIZE;
		@Default
		private DataStructureGeneratorItemReader.Options hashOptions = DataStructureGeneratorItemReader.Options
				.builder().build();
		@Default
		private CollectionGeneratorItemReader.Options listOptions = CollectionGeneratorItemReader.Options.builder()
				.build();
		@Default
		private CollectionGeneratorItemReader.Options setOptions = CollectionGeneratorItemReader.Options.builder()
				.build();
		@Default
		private CollectionGeneratorItemReader.Options streamOptions = CollectionGeneratorItemReader.Options.builder()
				.build();
		@Default
		private DataStructureGeneratorItemReader.Options stringOptions = DataStructureGeneratorItemReader.Options
				.builder().build();
		@Default
		private ZsetGeneratorItemReader.Options zsetOptions = ZsetGeneratorItemReader.Options.builder().build();

		public void to(int max) {
			hashOptions.to(max);
			listOptions.to(max);
			setOptions.to(max);
			streamOptions.to(max);
			stringOptions.to(max);
			zsetOptions.to(max);
		}

		public void keyPrefix(String prefix) {
			hashOptions.setKeyPrefix(prefix);
			listOptions.getDataStructureOptions().setKeyPrefix(prefix);
			setOptions.getDataStructureOptions().setKeyPrefix(prefix);
			streamOptions.getDataStructureOptions().setKeyPrefix(prefix);
			stringOptions.setKeyPrefix(prefix);
			zsetOptions.getCollectionOptions().getDataStructureOptions().setKeyPrefix(prefix);
		}

		public static class OptionsBuilder {

			public OptionsBuilder dataType(DataType... dataTypes) {
				dataTypes(Arrays.asList(dataTypes));
				return this;
			}
		}

	}

}
