package com.redis.spring.batch.support.generator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;

import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.support.SimpleFlow;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.SyncTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

import com.redis.spring.batch.RedisItemWriter;
import com.redis.spring.batch.support.DataStructure;
import com.redis.spring.batch.support.generator.CollectionGeneratorItemReader.CollectionOptions;
import com.redis.spring.batch.support.generator.DataStructureGeneratorItemReader.DataStructureOptions;
import com.redis.spring.batch.support.generator.StringGeneratorItemReader.StringOptions;
import com.redis.spring.batch.support.generator.ZsetGeneratorItemReader.ZsetOptions;

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
	private final JobRepository jobRepository;
	private final PlatformTransactionManager transactionManager;
	private final AbstractRedisClient client;
	@Getter
	private final GeneratorOptions options;

	public Generator(String id, JobRepository jobRepository, PlatformTransactionManager transactionManager,
			AbstractRedisClient client, GeneratorOptions options) {
		this.id = id;
		this.jobRepository = jobRepository;
		this.transactionManager = transactionManager;
		this.client = client;
		this.options = options;
	}

	@Override
	public JobExecution call() throws Exception {
		String name = id + "-" + NAME;
		StepBuilderFactory stepBuilderFactory = new StepBuilderFactory(jobRepository, transactionManager);
		List<SimpleFlow> flows = new ArrayList<>();
		for (DataType type : options.getDataTypes()) {
			flows.add(new FlowBuilder<SimpleFlow>(type + "-" + name).start(stepBuilderFactory.get(type + "-" + name)
					.<DataStructure<String>, DataStructure<String>>chunk(options.getChunkSize()).reader(reader(type))
					.writer(writer()).build()).build());
		}
		SimpleFlow flow = new FlowBuilder<SimpleFlow>(name).split(new SimpleAsyncTaskExecutor())
				.add(flows.toArray(new SimpleFlow[0])).build();
		SimpleJobLauncher launcher = new SimpleJobLauncher();
		launcher.setJobRepository(jobRepository);
		launcher.setTaskExecutor(new SyncTaskExecutor());
		return launcher.run(new JobBuilderFactory(jobRepository).get(name).start(flow).build().build(),
				new JobParameters());
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

	public static class GeneratorBuilder {

		private final String id;
		private final JobRepository jobRepository;
		private final PlatformTransactionManager transactionManager;
		private final AbstractRedisClient client;
		private GeneratorOptions options = GeneratorOptions.builder().build();

		public GeneratorBuilder(String id, JobRepository jobRepository, PlatformTransactionManager transactionManager,
				AbstractRedisClient client) {
			this.id = id;
			this.jobRepository = jobRepository;
			this.transactionManager = transactionManager;
			this.client = client;
		}

		public GeneratorBuilder dataTypes(DataType... dataTypes) {
			this.options.setDataTypes(Arrays.asList(dataTypes));
			return this;
		}

		public GeneratorBuilder options(GeneratorOptions options) {
			this.options = options;
			return this;
		}

		public GeneratorBuilder hash(DataStructureGeneratorItemReader.DataStructureOptions hashOptions) {
			this.options.setHashOptions(hashOptions);
			return this;
		}

		public GeneratorBuilder string(StringOptions stringOptions) {
			this.options.setStringOptions(stringOptions);
			return this;
		}

		public Generator build() {
			return new Generator(id, jobRepository, transactionManager, client, options);
		}

		public GeneratorBuilder start(long start) {
			this.options.start(start);
			return this;
		}

		public GeneratorBuilder end(long end) {
			this.options.end(end);
			return this;
		}

		public GeneratorBuilder keyPrefix(String prefix) {
			this.options.keyPrefix(prefix);
			return this;
		}

	}

	@Data
	@Builder
	public static class GeneratorOptions {

		public static final int DEFAULT_CHUNK_SIZE = 50;

		public static final DataType[] DEFAULT_DATATYPES = DataType.values();

		@Default
		private List<DataType> dataTypes = Arrays.asList(DEFAULT_DATATYPES);
		@Default
		private int chunkSize = DEFAULT_CHUNK_SIZE;
		@Default
		private DataStructureOptions hashOptions = DataStructureOptions.builder().build();
		@Default
		private CollectionOptions listOptions = CollectionOptions.builder().build();
		@Default
		private CollectionOptions setOptions = CollectionOptions.builder().build();
		@Default
		private CollectionOptions streamOptions = CollectionOptions.builder().build();
		@Default
		private StringOptions stringOptions = StringOptions.builder().build();
		@Default
		private ZsetOptions zsetOptions = ZsetOptions.builder().build();

		public void start(long start) {
			hashOptions.setStart(start);
			listOptions.setStart(start);
			setOptions.setStart(start);
			streamOptions.setStart(start);
			stringOptions.setStart(start);
			zsetOptions.setStart(start);
		}

		public void end(long end) {
			hashOptions.setEnd(end);
			listOptions.setEnd(end);
			setOptions.setEnd(end);
			streamOptions.setEnd(end);
			stringOptions.setEnd(end);
			zsetOptions.setEnd(end);
		}

		public void keyPrefix(String prefix) {
			hashOptions.setKeyPrefix(prefix);
			listOptions.setKeyPrefix(prefix);
			setOptions.setKeyPrefix(prefix);
			streamOptions.setKeyPrefix(prefix);
			stringOptions.setKeyPrefix(prefix);
			zsetOptions.setKeyPrefix(prefix);
		}

		public static class GeneratorOptionsBuilder {

			public GeneratorOptionsBuilder dataType(DataType... dataTypes) {
				dataTypes(Arrays.asList(dataTypes));
				return this;
			}
		}

	}

}
