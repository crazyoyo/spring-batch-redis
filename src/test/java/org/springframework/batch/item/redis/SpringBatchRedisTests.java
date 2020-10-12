package org.springframework.batch.item.redis;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.DefaultBufferedReaderFactory;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.separator.DefaultRecordSeparatorPolicy;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.redis.support.KeyDump;
import org.springframework.batch.item.redis.support.KeyValue;
import org.springframework.batch.item.redis.support.KeyValueItemComparator;
import org.springframework.batch.item.redis.support.LiveKeyItemReader;
import org.springframework.batch.item.support.ListItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.test.context.junit4.SpringRunner;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;

@SpringBootTest(classes = BatchTestApplication.class)
@RunWith(SpringRunner.class)
@SuppressWarnings("rawtypes")
public class SpringBatchRedisTests {

	private static final DockerImageName DOCKER_IMAGE_NAME = DockerImageName.parse("redis:5.0.3-alpine");

	private static GenericContainer sourceRedis;
	private static GenericContainer targetRedis;
	private static RedisURI sourceRedisURI;
	private static RedisURI targetRedisURI;
	private static RedisClient sourceRedisClient;
	private static RedisClient targetRedisClient;

	@BeforeAll
	public static void setup() {
		sourceRedis = container(6379);
		sourceRedisURI = RedisURI.create(sourceRedis.getHost(), sourceRedis.getFirstMappedPort());
		sourceRedisClient = RedisClient.create(sourceRedisURI);
		targetRedis = container(6380);
		targetRedisURI = RedisURI.create(targetRedis.getHost(), targetRedis.getFirstMappedPort());
		targetRedisClient = RedisClient.create(targetRedisURI);
	}

	@AfterAll
	public static void teardown() {
		targetRedisClient.shutdown();
		targetRedis.stop();
		sourceRedisClient.shutdown();
		sourceRedis.stop();
	}

	@SuppressWarnings("resource")
	private static GenericContainer container(int port) {
		GenericContainer container = new GenericContainer<>(DOCKER_IMAGE_NAME).withExposedPorts(6379);
		container.start();
		return container;
	}

	@BeforeEach
	public void flush() {
		StatefulRedisConnection<String, String> sourceConnection = sourceRedisClient.connect();
		sourceConnection.sync().flushall();
		sourceConnection.sync().configSet("notify-keyspace-events", "AK");
		sourceConnection.close();
		StatefulRedisConnection<String, String> targetConnection = targetRedisClient.connect();
		targetConnection.sync().flushall();
		targetConnection.close();
	}

	@Autowired
	private JobLauncher jobLauncher;
	@Autowired
	private JobLauncher asyncJobLauncher;
	@Autowired
	private JobBuilderFactory jobBuilderFactory;
	@Autowired
	private StepBuilderFactory stepBuilderFactory;

	private FlatFileItemReader<Map<String, String>> fileReader(Resource resource) throws IOException {
		FlatFileItemReaderBuilder<Map<String, String>> builder = new FlatFileItemReaderBuilder<>();
		builder.name("flat-file-reader");
		builder.resource(resource);
		builder.saveState(false);
		builder.linesToSkip(1);
		builder.fieldSetMapper(new MapFieldSetMapper());
		builder.recordSeparatorPolicy(new DefaultRecordSeparatorPolicy());
		FlatFileItemReaderBuilder.DelimitedBuilder<Map<String, String>> delimitedBuilder = builder.delimited();
		BufferedReader reader = new DefaultBufferedReaderFactory().create(resource, FlatFileItemReader.DEFAULT_CHARSET);
		DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();
		tokenizer.setDelimiter(DelimitedLineTokenizer.DELIMITER_COMMA);
		String[] fieldNames = tokenizer.tokenize(reader.readLine()).getValues();
		delimitedBuilder.names(fieldNames);
		return builder.build();
	}

	@Test
	public void testValueReader() throws Exception {
		FlatFileItemReader<Map<String, String>> fileReader = fileReader(new ClassPathResource("beers.csv"));
		StatefulRedisConnection<String, String> sourceConnection = sourceRedisClient.connect();
		ItemWriter<Map<String, String>> hmsetWriter = new ItemWriter<Map<String, String>>() {
			public void write(List<? extends Map<String, String>> items) throws Exception {
				for (Map<String, String> item : items) {
					sourceConnection.sync().hmset(item.get(Beers.FIELD_ID), item);
				}
			}
		};
		run("scan-reader-populate", fileReader, hmsetWriter);
		RedisKeyValueItemReader<String> reader = RedisKeyValueItemReader.builder().redisURI(sourceRedisURI).build();
		ListItemWriter<KeyValue<String>> writer = new ListItemWriter<>();
		JobExecution execution = run("scan-reader", reader, writer);
		Assert.assertTrue(execution.getAllFailureExceptions().isEmpty());
		Assert.assertEquals(Beers.SIZE, writer.getWrittenItems().size());
	}

	@Test
	public void testReplication() throws Exception {
		DataPopulator.builder().client(sourceRedisClient).start(0).end(1039).build().run();
		RedisKeyDumpItemReader<String> reader = RedisKeyDumpItemReader.builder().redisURI(sourceRedisURI).build();
		RedisKeyDumpItemWriter<String, String> writer = RedisKeyDumpItemWriter.builder().redisURI(targetRedisURI)
				.replace(true).build();
		run("replication", reader, writer);
		compare("replication-comparison");
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testLiveReplication() throws Exception {
		DataPopulator.builder().client(sourceRedisClient).start(0).end(1000).build().run();
		RedisKeyDumpItemReader<String> reader = RedisKeyDumpItemReader.builder().redisURI(sourceRedisURI).live(true)
				.threads(2).build();
		LiveKeyItemReader<String, String> keyReader = (LiveKeyItemReader<String, String>) reader.getKeyReader();
		RedisKeyDumpItemWriter<String, String> writer = RedisKeyDumpItemWriter.builder().redisURI(targetRedisURI)
				.replace(true).build();
		Job job = job("live-replication", reader, writer);
		JobExecution execution = asyncJobLauncher.run(job, new JobParameters());
		while (!keyReader.isRunning()) {
			Thread.sleep(1);
		}
		DataPopulator.builder().client(sourceRedisClient).start(1000).end(2000).sleep(1L).build().run();
		Thread.sleep(100);
		reader.flush();
		Thread.sleep(100);
		keyReader.stop();
		while (execution.isRunning()) {
			Thread.sleep(10);
		}
		compare("live-replication-comparison");
	}

	private void compare(String name) throws Exception {
		StatefulRedisConnection<String, String> sourceConnection = sourceRedisClient.connect();
		RedisCommands<String, String> sourceCommands = sourceConnection.sync();
		StatefulRedisConnection<String, String> targetConnection = targetRedisClient.connect();
		RedisCommands<String, String> targetCommands = targetConnection.sync();
		Assert.assertEquals(sourceCommands.dbsize(), targetCommands.dbsize());
		RedisKeyValueItemReader<String> reader = RedisKeyValueItemReader.builder().redisURI(sourceRedisURI).build();
		KeyValueItemComparator<String> comparator = new KeyValueItemComparator<>(reader.getKeyValueProcessor(), 1);
		run(name, reader, comparator);
		Assert.assertEquals(Math.toIntExact(sourceCommands.dbsize()), comparator.getOk().size());
		targetConnection.close();
		sourceConnection.close();
	}

	private <T> JobExecution run(String name, ItemReader<T> reader, ItemWriter<T> writer) throws Exception {
		return jobLauncher.run(job(name, reader, writer), new JobParameters());
	}

	private <I, O> Job job(String name, ItemReader<? extends I> reader, ItemWriter<? super O> writer) {
		return jobBuilderFactory.get(name + "-job").start(step(name, reader, writer)).build();
	}

	private <I, O> Step step(String name, ItemReader<? extends I> reader, ItemWriter<? super O> writer) {
		return stepBuilderFactory.get(name + "-step").<I, O>chunk(50).reader(reader).writer(writer).build();
	}

	public void usage() {
		RedisURI sourceURI = RedisURI.create("redis://source:6379");
		RedisKeyDumpItemReader<String> reader = RedisKeyDumpItemReader.builder().redisURI(sourceURI).live(true)
				.threads(2).build();
		RedisURI targetURI = RedisURI.create("rediss://target:6379");
		RedisKeyDumpItemWriter<String, String> writer = RedisKeyDumpItemWriter.builder().redisURI(targetURI)
				.replace(true).build();
		TaskletStep step = stepBuilderFactory.get("step").<KeyDump<String>, KeyDump<String>>chunk(50).reader(reader)
				.writer(writer).build();
		jobBuilderFactory.get("job").start(step).build();
	}

}