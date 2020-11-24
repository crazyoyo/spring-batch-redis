package org.springframework.batch.item.redis;

import java.io.BufferedReader;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.DefaultBufferedReaderFactory;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.separator.DefaultRecordSeparatorPolicy;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.redis.support.DataStructure;
import org.springframework.batch.item.redis.support.DataType;
import org.springframework.batch.item.redis.support.KeyMaker;
import org.springframework.batch.item.redis.support.KeyValue;
import org.springframework.batch.item.redis.support.LettuceCommandLatencyRecorder;
import org.springframework.batch.item.redis.support.LiveKeyItemReader;
import org.springframework.batch.item.redis.support.Transfer;
import org.springframework.batch.item.redis.support.TransferExecution;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.batch.item.support.ListItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.test.context.junit4.SpringRunner;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import io.lettuce.core.Range;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.XReadArgs.StreamOffset;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.event.DefaultEventPublisherOptions;
import io.lettuce.core.resource.DefaultClientResources;
import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.jmx.JmxConfig;
import io.micrometer.jmx.JmxMeterRegistry;
import lombok.extern.slf4j.Slf4j;

@SpringBootTest(classes = BatchTestApplication.class)
@RunWith(SpringRunner.class)
@SuppressWarnings("rawtypes")
@Slf4j
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
	public void testKeyValueItemReaderProgress() {
		DataGenerator.builder().client(sourceRedisClient).start(0).end(100).build().run();
		RedisDataStructureItemReader reader = RedisDataStructureItemReader.builder().uri(sourceRedisURI).build();
		reader.open(new ExecutionContext());
		long total = reader.available();
		Assertions.assertEquals(sourceRedisClient.connect().sync().dbsize(), total, 10);
		reader.close();
	}

	@Test
	public void testDataStructureReader() throws Exception {
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
		RedisDataStructureItemReader reader = RedisDataStructureItemReader.builder().uri(sourceRedisURI).build();
		ListItemWriter<DataStructure> writer = new ListItemWriter<>();
		JobExecution execution = run("scan-reader", reader, writer);
		Assert.assertTrue(execution.getAllFailureExceptions().isEmpty());
		Assert.assertEquals(Beers.SIZE, writer.getWrittenItems().size());
	}

	@Test
	public void testStreamReader() throws Exception {
		DataGenerator.builder().client(sourceRedisClient).start(0).end(100).build().run();
		RedisStreamItemReader reader = RedisStreamItemReader.builder().uri(sourceRedisURI)
				.offset(StreamOffset.from("stream:0", "0-0")).build();
		reader.setMaxItemCount(10);
		ListItemWriter<StreamMessage<String, String>> writer = new ListItemWriter<>();
		run("stream-reader", reader, writer);
		Assertions.assertEquals(10, writer.getWrittenItems().size());
		List<? extends StreamMessage<String, String>> items = writer.getWrittenItems();
		for (StreamMessage<String, String> message : items) {
			Assertions.assertTrue(message.getBody().containsKey("field1"));
			Assertions.assertTrue(message.getBody().containsKey("field2"));
		}
	}

	@Test
	public void testStreamWriter() throws Exception {
		String stream = "stream:0";
		List<Map<String, String>> messages = new ArrayList<>();
		for (int index = 0; index < 100; index++) {
			Map<String, String> body = new HashMap<>();
			body.put("field1", "value1");
			body.put("field2", "value2");
			messages.add(body);
		}
		ListItemReader<Map<String, String>> reader = new ListItemReader<>(messages);
		RedisStreamItemWriter<Map<String, String>> writer = RedisStreamItemWriter.<Map<String, String>>builder()
				.uri(targetRedisURI).keyConverter(i -> stream).bodyConverter(i -> i).build();
		run("stream-writer", reader, writer);
		Assertions.assertEquals(messages.size(), targetRedisClient.connect().sync().xlen(stream));
		List<StreamMessage<String, String>> xrange = targetRedisClient.connect().sync().xrange(stream,
				Range.create("-", "+"));
		for (int index = 0; index < xrange.size(); index++) {
			StreamMessage<String, String> message = xrange.get(index);
			Assertions.assertEquals(messages.get(index), message.getBody());
		}
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testHashWriter() throws Exception {
		List<Map<String, String>> maps = new ArrayList<>();
		for (int index = 0; index < 100; index++) {
			Map<String, String> body = new HashMap<>();
			body.put("id", String.valueOf(index));
			body.put("field1", "value1");
			body.put("field2", "value2");
			maps.add(body);
		}
		ListItemReader<Map<String, String>> reader = new ListItemReader<>(maps);
		KeyMaker<Map<String, String>> keyConverter = KeyMaker.<Map<String, String>>builder().prefix("hash")
				.extractors(h -> h.remove("id")).build();
		RedisHashItemWriter<Map<String, String>> writer = RedisHashItemWriter.<Map<String, String>>builder()
				.uri(targetRedisURI).keyConverter(keyConverter).mapConverter(m -> m).build();
		run("hash-writer", reader, writer);
		StatefulRedisConnection<String, String> connection = targetRedisClient.connect();
		Assertions.assertEquals(maps.size(), connection.sync().keys("hash:*").size());
		for (int index = 0; index < maps.size(); index++) {
			Map<String, String> hash = connection.sync().hgetall("hash:" + index);
			Assertions.assertEquals(maps.get(index), hash);
		}
	}

	@Test
	public void testDataStructureWriter() throws Exception {
		List<DataStructure> list = new ArrayList<>();
		long count = 100;
		for (int index = 0; index < count; index++) {
			DataStructure keyValue = new DataStructure();
			keyValue.setKey("hash:" + index);
			keyValue.setType(DataType.HASH);
			keyValue.setValue(Map.of("field1", "value1", "field2", "value2"));
			list.add(keyValue);
		}
		ListItemReader<DataStructure> reader = new ListItemReader<>(list);
		RedisDataStructureItemWriter writer = RedisDataStructureItemWriter.builder().uri(targetRedisURI).build();
		run("value-writer", reader, writer);
		StatefulRedisConnection<String, String> connection = targetRedisClient.connect();
		List<String> keys = connection.sync().keys("hash:*");
		Assertions.assertEquals(count, keys.size());
	}

	@Test
	public void testReplication() throws Exception {
		DataGenerator.builder().client(sourceRedisClient).start(0).end(10000).build().run();
		RedisDumpItemReader reader = RedisDumpItemReader.builder().uri(sourceRedisURI).build();
		RedisDumpItemWriter writer = RedisDumpItemWriter.builder().uri(targetRedisURI).replace(true).build();
		run("replication", reader, writer);
		compare("replication-comparison");
	}

	@Test
	public void testLiveReplication() throws Exception {
		RedisDumpItemReader reader = RedisDumpItemReader.builder().uri(sourceRedisURI).live(true).threads(2).build();
		RedisDumpItemWriter writer = RedisDumpItemWriter.builder().uri(targetRedisURI).replace(true).build();
		Job job = job("live-replication", reader, writer);
		JobExecution execution = asyncJobLauncher.run(job, new JobParameters());
		LiveKeyItemReader keyReader = (LiveKeyItemReader) reader.getKeyReader();
		while (!keyReader.isRunning()) {
			Thread.sleep(1);
		}
		DataGenerator.builder().client(sourceRedisClient).start(1000).end(2000).sleep(1L).build().run();
		Thread.sleep(100);
		reader.flush();
		Thread.sleep(100);
		keyReader.stop();
		while (execution.isRunning()) {
			Thread.sleep(10);
		}
		compare("live-replication-comparison");
	}

	@Test
	public void testLiveReplicationTransfer() throws Exception {
		RedisDumpItemReader reader = RedisDumpItemReader.builder().uri(sourceRedisURI).live(true).threads(2).build();
		RedisDumpItemWriter writer = RedisDumpItemWriter.builder().uri(targetRedisURI).replace(true).build();
		Transfer<KeyValue<byte[]>, KeyValue<byte[]>> transfer = Transfer.<KeyValue<byte[]>, KeyValue<byte[]>>builder()
				.name("live-replication").reader(reader).writer(writer).batch(100).build();
		TransferExecution<KeyValue<byte[]>, KeyValue<byte[]>> execution = transfer.execute();
		DataGenerator.builder().client(sourceRedisClient).start(1000).end(2000).build().run();
		Thread.sleep(100);
		execution.flush();
		Thread.sleep(100);
		execution.stop();
//		while (execution.isRunning()) {
//			Thread.sleep(10);
//		}
		compare("live-replication-comparison");
	}

	private void compare(String name) throws Exception {
		RedisCommands<String, String> sourceCommands = sourceRedisClient.connect().sync();
		Assert.assertEquals(sourceCommands.dbsize(), targetRedisClient.connect().sync().dbsize());
		RedisDataStructureItemReader sourceReader = RedisDataStructureItemReader.builder().uri(sourceRedisURI).build();
		RedisDataStructureItemReader targetReader = RedisDataStructureItemReader.builder().uri(targetRedisURI).build();
		RedisDatabaseComparator comparator = RedisDatabaseComparator.builder().sourceReader(sourceReader)
				.targetReader(targetReader).build();
		comparator.execute().getFuture().join();
		Assert.assertEquals(Math.toIntExact(sourceCommands.dbsize()), comparator.getOk().size());
	}

	private <T> JobExecution run(String name, ItemReader<T> reader, ItemWriter<T> writer) throws Exception {
		return jobLauncher.run(job(name, reader, writer), new JobParameters());
	}

	private <I, O> Job job(String name, ItemReader<? extends I> reader, ItemWriter<? super O> writer) {
		TaskletStep step = stepBuilderFactory.get(name + "-step").<I, O>chunk(50).reader(reader).writer(writer).build();
		return jobBuilderFactory.get(name + "-job").start(step).build();
	}

	@Test
	public void testMetrics() throws Exception {
		Metrics.addRegistry(new JmxMeterRegistry(new JmxConfig() {
			@Override
			public String get(String s) {
				return null;
			}
		}, Clock.SYSTEM));
		DataGenerator.builder().client(sourceRedisClient).start(0).end(10000).build().run();
		DefaultClientResources clientResources = DefaultClientResources.builder()
				.commandLatencyPublisherOptions(
						DefaultEventPublisherOptions.builder().eventEmitInterval(Duration.ofSeconds(1)).build())
				.commandLatencyRecorder(new LettuceCommandLatencyRecorder(Metrics.globalRegistry, true)).build();
		RedisDataStructureItemReader reader = RedisDataStructureItemReader.builder().uri(sourceRedisURI)
				.clientResources(clientResources).build();
		ListItemWriter<DataStructure> writer = new ListItemWriter<>();
		Transfer<DataStructure, DataStructure> transfer = Transfer.<DataStructure, DataStructure>builder()
				.name("transfer1").reader(reader).writer(writer).build();
		transfer.execute().getFuture().join();
		log.info("Job completed");
	}

}
