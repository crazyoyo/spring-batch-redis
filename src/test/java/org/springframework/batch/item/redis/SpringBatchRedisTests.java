package org.springframework.batch.item.redis;

import java.io.BufferedReader;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

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
import org.springframework.batch.item.redis.support.LiveKeyItemReader;
import org.springframework.batch.item.redis.support.LiveReaderOptions;
import org.springframework.batch.item.redis.support.MetricsUtils;
import org.springframework.batch.item.redis.support.MultiTransferExecution;
import org.springframework.batch.item.redis.support.MultiTransferExecutionListenerAdapter;
import org.springframework.batch.item.redis.support.Transfer;
import org.springframework.batch.item.redis.support.TransferExecution;
import org.springframework.batch.item.redis.support.TransferExecutionListener;
import org.springframework.batch.item.redis.support.TransferOptions;
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
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.search.Search;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import lombok.extern.slf4j.Slf4j;

@SpringBootTest(classes = BatchTestApplication.class)
@RunWith(SpringRunner.class)
@SuppressWarnings("rawtypes")
@Slf4j
public class SpringBatchRedisTests {

	private static final DockerImageName REDIS_IMAGE_NAME = DockerImageName.parse("redis:5.0.3-alpine");
	private static GenericContainer sourceRedis;
	private static GenericContainer targetRedis;
	private static RedisClient sourceRedisClient;
	private static RedisClient targetRedisClient;
	private static StatefulRedisConnection<String, String> sourceConnection;
	private static StatefulRedisConnection<String, String> targetConnection;
	private static RedisCommands<String, String> sourceSync;
	private static RedisCommands<String, String> targetSync;

	@BeforeAll
	public static void setup() {
		sourceRedis = container();
		sourceRedisClient = RedisClient
				.create(RedisURI.create(sourceRedis.getHost(), sourceRedis.getFirstMappedPort()));
		sourceConnection = sourceRedisClient.connect();
		sourceSync = sourceConnection.sync();
		sourceSync.configSet("notify-keyspace-events", "AK");
		targetRedis = container();
		targetRedisClient = RedisClient
				.create(RedisURI.create(targetRedis.getHost(), targetRedis.getFirstMappedPort()));
		targetConnection = targetRedisClient.connect();
		targetSync = targetConnection.sync();
	}

	@AfterAll
	public static void teardown() {
		targetConnection.close();
		targetRedisClient.shutdown();
		targetRedis.stop();
		sourceConnection.close();
		sourceRedisClient.shutdown();
		sourceRedis.stop();
	}

	@SuppressWarnings("resource")
	private static GenericContainer container() {
		GenericContainer container = new GenericContainer<>(REDIS_IMAGE_NAME).withExposedPorts(6379);
		container.start();
		return container;
	}

	@BeforeEach
	public void flush() {
		sourceSync.flushall();
		targetSync.flushall();
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
		DataGenerator.builder(sourceRedisClient).options(DataGeneratorOptions.builder().end(100).build()).build().run();
		DataStructureItemReader reader = DataStructureItemReader.builder(sourceRedisClient).build();
		reader.open(new ExecutionContext());
		long total = reader.available();
		Assertions.assertEquals(sourceSync.dbsize(), total, 10);
		reader.close();
	}

	@Test
	public void testDataStructureReader() throws Exception {
		FlatFileItemReader<Map<String, String>> fileReader = fileReader(new ClassPathResource("beers.csv"));
		ItemWriter<Map<String, String>> hmsetWriter = new ItemWriter<Map<String, String>>() {

			public void write(List<? extends Map<String, String>> items) throws Exception {
				for (Map<String, String> item : items) {
					sourceSync.hmset(item.get(Beers.FIELD_ID), item);
				}
			}

		};
		run("scan-reader-populate", fileReader, hmsetWriter);
		DataStructureItemReader reader = DataStructureItemReader.builder(sourceRedisClient).build();
		ListItemWriter<DataStructure> writer = new ListItemWriter<>();
		JobExecution execution = run("scan-reader", reader, writer);
		Assert.assertTrue(execution.getAllFailureExceptions().isEmpty());
		Assert.assertEquals(Beers.SIZE, writer.getWrittenItems().size());
	}

	@Test
	public void testStreamReader() throws Exception {
		DataGenerator.builder(sourceRedisClient).options(DataGeneratorOptions.builder().end(100).build()).build().run();
		StreamItemReader reader = StreamItemReader.builder(sourceRedisClient)
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
		StreamItemWriter<Map<String, String>> writer = StreamItemWriter.<Map<String, String>>builder(targetRedisClient)
				.keyConverter(i -> stream).bodyConverter(i -> i).build();
		run("stream-writer", reader, writer);
		Assertions.assertEquals(messages.size(), targetSync.xlen(stream));
		List<StreamMessage<String, String>> xrange = targetSync.xrange(stream, Range.create("-", "+"));
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
		HashItemWriter<Map<String, String>> writer = HashItemWriter.<Map<String, String>>builder(targetRedisClient)
				.keyConverter(keyConverter).mapConverter(m -> m).build();
		run("hash-writer", reader, writer);
		Assertions.assertEquals(maps.size(), targetSync.keys("hash:*").size());
		for (int index = 0; index < maps.size(); index++) {
			Map<String, String> hash = targetSync.hgetall("hash:" + index);
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
		DataStructureItemWriter writer = DataStructureItemWriter.builder(targetRedisClient).build();
		run("value-writer", reader, writer);
		List<String> keys = targetSync.keys("hash:*");
		Assertions.assertEquals(count, keys.size());
	}

	@Test
	public void testReplication() throws Exception {
		DataGenerator.builder(sourceRedisClient).options(DataGeneratorOptions.builder().end(10000).build()).build()
				.run();
		KeyDumpItemReader reader = KeyDumpItemReader.builder(sourceRedisClient).build();
		KeyDumpItemWriter writer = KeyDumpItemWriter.builder(targetRedisClient).replace(true).build();
		run("replication", reader, writer);
		compare("replication-comparison");
	}

	@Test
	public void testLiveReplication() throws Exception {
		LiveReaderOptions options = LiveReaderOptions.builder()
				.transferOptions(TransferOptions.builder().threads(2).build()).build();
		LiveKeyDumpItemReader reader = LiveKeyDumpItemReader.builder(sourceRedisClient).options(options).build();
		KeyDumpItemWriter writer = KeyDumpItemWriter.builder(targetRedisClient).replace(true).build();
		Job job = job("live-replication", reader, writer);
		JobExecution execution = asyncJobLauncher.run(job, new JobParameters());
		Thread.sleep(100);
		LiveKeyItemReader keyReader = (LiveKeyItemReader) reader.getKeyReader();
		DataGenerator.builder(sourceRedisClient).options(DataGeneratorOptions.builder().end(2).sleep(1L).build())
				.build().run();
		Thread.sleep(100);
		keyReader.stop();
		while (execution.isRunning()) {
			Thread.sleep(10);
		}
		compare("live-replication-comparison");
	}

	@Test
	public void testLiveReplicationTransfer() throws Exception {
		LiveReaderOptions options = LiveReaderOptions.builder()
				.transferOptions(TransferOptions.builder().threads(2).flushInterval(Duration.ofMillis(10)).build())
				.build();
		LiveKeyDumpItemReader reader = LiveKeyDumpItemReader.builder(sourceRedisClient).options(options).build();
		KeyDumpItemWriter writer = KeyDumpItemWriter.builder(targetRedisClient).replace(true).build();
		Transfer<KeyValue<byte[]>, KeyValue<byte[]>> transfer = Transfer.<KeyValue<byte[]>, KeyValue<byte[]>>builder()
				.name("live-replication").reader(reader).writer(writer)
				.options(TransferOptions.builder().batch(100).flushInterval(Duration.ofMillis(50)).build()).build();
		TransferExecution<KeyValue<byte[]>, KeyValue<byte[]>> execution = new TransferExecution<>(transfer);
		CompletableFuture<Void> future = execution.start();
		DataGenerator.builder(sourceRedisClient).options(DataGeneratorOptions.builder().end(3).build()).build().run();
		Thread.sleep(100);
		LiveKeyItemReader keyReader = (LiveKeyItemReader) reader.getKeyReader();
		keyReader.stop();
		execution.stop();
		future.get();
		compare("live-replication-comparison");
	}

	private void compare(String name) throws Exception {
		Assert.assertEquals(sourceSync.dbsize(), targetSync.dbsize());
		DatabaseComparator comparator = DatabaseComparator.builder(sourceRedisClient, targetRedisClient).build();
		comparator.execution().start().get();
		Assert.assertEquals(Math.toIntExact(sourceSync.dbsize()), comparator.getOk().size());
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
		SimpleMeterRegistry registry = new SimpleMeterRegistry();
		Metrics.addRegistry(registry);
		DataGenerator.builder(sourceRedisClient).options(DataGeneratorOptions.builder().end(10).build()).build().run();
		DataStructureItemReader reader = DataStructureItemReader.builder(sourceRedisClient).build();
		Transfer<DataStructure, DataStructure> transfer = Transfer.<DataStructure, DataStructure>builder()
				.name("metrics").reader(reader).writer(ThrottledWriter.<DataStructure>builder().build()).build();
		new TransferExecution<>(transfer).start().get();
		Search search = registry.find(MetricsUtils.METRICS_PREFIX + "item.read");
		Assertions.assertFalse(search.timers().isEmpty());
		log.info("Job completed");
	}

	@Test
	public void testTransferListener() throws Exception {
		DataGenerator.builder(sourceRedisClient).options(DataGeneratorOptions.builder().end(10000).build()).build()
				.run();
		DataStructureItemReader reader = DataStructureItemReader.builder(sourceRedisClient).build();
		Transfer<DataStructure, DataStructure> transfer = Transfer.<DataStructure, DataStructure>builder()
				.name("transfer-listener").reader(reader).writer(ThrottledWriter.<DataStructure>builder().build())
				.build();
		AtomicLong updateCount = new AtomicLong();
		AtomicBoolean complete = new AtomicBoolean();
		TransferExecution<DataStructure, DataStructure> execution = new TransferExecution<>(transfer);
		execution.addListener(new TransferExecutionListener() {

			@Override
			public void onMessage(String message) {
			}

			@Override
			public void onUpdate(long count) {
				updateCount.set(count);
			}

			@Override
			public void onError(Throwable throwable) {
				Assertions.fail("Exception thrown during transfer", throwable);
			}

			@Override
			public void onComplete() {
				complete.set(true);
			}
		});
		execution.start().get();
		Assertions.assertTrue(complete.get());
		Assertions.assertEquals(20030, updateCount.get());
	}

	@Test
	public void testMultiTransferListener() throws Exception {
		DataGenerator.builder(sourceRedisClient).options(DataGeneratorOptions.builder().end(10000).build()).build()
				.run();
		DataStructureItemReader reader1 = DataStructureItemReader.builder(sourceRedisClient).build();
		ListItemWriter<DataStructure> writer1 = new ListItemWriter<>();
		Transfer<DataStructure, DataStructure> transfer1 = Transfer.<DataStructure, DataStructure>builder()
				.name("transfer1").reader(reader1).writer(writer1).build();
		DataStructureItemReader reader2 = DataStructureItemReader.builder(sourceRedisClient).build();
		ListItemWriter<DataStructure> writer2 = new ListItemWriter<>();
		Transfer<DataStructure, DataStructure> transfer2 = Transfer.<DataStructure, DataStructure>builder()
				.name("transfer-listener").reader(reader2).writer(writer2).build();
		MultiTransferExecution execution = new MultiTransferExecution(Arrays.asList(transfer1, transfer2));
		Set<Transfer<?, ?>> startedTransfers = new HashSet<>();
		Set<Transfer<?, ?>> completedTransfers = new HashSet<>();
		AtomicBoolean started = new AtomicBoolean();
		AtomicBoolean completed = new AtomicBoolean();
		execution.addListener(new MultiTransferExecutionListenerAdapter() {

			@Override
			public void onStart(TransferExecution<?, ?> execution) {
				startedTransfers.add(execution.getTransfer());
			}

			@Override
			public void onStart() {
				started.set(true);
			}

			@Override
			public void onError(TransferExecution<?, ?> execution, Throwable throwable) {
				Assertions.fail("Exception thrown during transfer", throwable);
			}

			@Override
			public void onComplete() {
				completed.set(true);
			}

			@Override
			public void onComplete(TransferExecution<?, ?> execution) {
				completedTransfers.add(execution.getTransfer());
			}
		});
		execution.start().get();
		Assertions.assertTrue(started.get());
		Assertions.assertTrue(completed.get());
		Assertions.assertTrue(startedTransfers.contains(transfer1));
		Assertions.assertTrue(startedTransfers.contains(transfer2));
		Assertions.assertTrue(completedTransfers.contains(transfer1));
		Assertions.assertTrue(completedTransfers.contains(transfer2));

	}

}
