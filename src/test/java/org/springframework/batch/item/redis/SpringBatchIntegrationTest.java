package org.springframework.batch.item.redis;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.junit.Assert;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.file.DefaultBufferedReaderFactory;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.separator.DefaultRecordSeparatorPolicy;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.redis.support.*;
import org.springframework.batch.item.redis.support.commands.Hmset;
import org.springframework.batch.item.redis.support.commands.HmsetArgs;
import org.springframework.batch.item.redis.support.commands.Restore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.BufferedReader;
import java.io.IOException;
import java.time.Duration;
import java.util.*;


@SpringBootTest(classes = BatchTestApplication.class)
@RunWith(SpringRunner.class)
public class SpringBatchIntegrationTest extends BaseTest {

    private final static int INITIAL_SIZE = 2410;

    @Autowired
    private JobLauncher jobLauncher;
    @Autowired
    private JobLauncher asyncJobLauncher;
    @Autowired
    private JobBuilderFactory jobBuilderFactory;
    @Autowired
    private StepBuilderFactory stepBuilderFactory;
    @Autowired
    private RedisCommands<String, String> commands;
    @Autowired
    private RedisClient client;
    @Autowired
    private RedisClient targetClient;
    @Autowired
    private GenericObjectPool<StatefulRedisConnection<String, String>> connectionPool;
    @Autowired
    private GenericObjectPool<StatefulRedisConnection<String, String>> targetConnectionPool;


    private final static Logger log = LoggerFactory.getLogger(SpringBatchIntegrationTest.class);

    private void redisWriter(String name) throws IOException, JobParametersInvalidException, JobExecutionAlreadyRunningException, JobRestartException, JobInstanceAlreadyCompleteException {
        FlatFileItemReader<Map<String, String>> reader = fileReader(new ClassPathResource("beers.csv"));
        ItemProcessor<Map<String, String>, HmsetArgs<String, String>> processor = m -> new HmsetArgs<>(m.get(Beers.FIELD_ID), m);
        RedisItemWriter<String, String, HmsetArgs<String, String>> writer = RedisItemWriter.<String, String, HmsetArgs<String, String>>builder().command(new Hmset<>()).pool(connectionPool).build();
        TaskletStep step = stepBuilderFactory.get(name + "-step").<Map<String, String>, HmsetArgs<String, String>>chunk(10).reader(reader).processor(processor).writer(writer).build();
        Job job = jobBuilderFactory.get(name + "-job").start(step).build();
        jobLauncher.run(job, new JobParameters());
    }

    @Test
    public void testRedisWriter() throws Exception {
        redisWriter("redis-writer");
        List<String> keys = commands.keys("*");
        Assert.assertEquals(INITIAL_SIZE, keys.size());
    }

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
        redisWriter("scan-reader-populate");
        List<KeyValue<String>> values = new ArrayList<>();
        RedisItemReader<String, KeyValue<String>> reader = RedisItemReader.valueReaderBuilder().client(client).build();
        TaskletStep step = stepBuilderFactory.get("scan-reader-step").<KeyValue<String>, KeyValue<String>>chunk(10).reader(reader).writer(values::addAll).build();
        Job job = jobBuilderFactory.get("scan-reader-job").start(step).build();
        JobExecution execution = jobLauncher.run(job, new JobParameters());
        Assert.assertTrue(execution.getAllFailureExceptions().isEmpty());
        Assert.assertEquals(INITIAL_SIZE, values.size());
    }

    @Test
    public void testLiveValueReader() throws Exception {
        RedisItemReader<String, KeyValue<String>> reader = RedisItemReader.liveValueReaderBuilder().client(client).threads(2).flushPeriod(Duration.ofMillis(50)).build();
        List<KeyValue<String>> values = new ArrayList<>();
        TaskletStep step = stepBuilderFactory.get("keyspace-reader-step").<KeyValue<String>, KeyValue<String>>chunk(10).reader(reader).writer(values::addAll).build();
        Job job = jobBuilderFactory.get("keyspace-reader-job").start(step).build();
        JobExecution execution = asyncJobLauncher.run(job, new JobParameters());
        Thread.sleep(100);
        RedisCommands<String, String> commands = client.connect().sync();
        int count = 13;
        for (int index = 0; index < count; index++) {
            commands.set("string:" + index, "value" + index);
            Thread.sleep(1);
        }
        Thread.sleep(300);
        ((RedisKeyspaceNotificationItemReader<String, String>) ((MultiplexingItemReader<String>) reader.getKeyReader()).getReaders().get(0)).stop();
        while (execution.isRunning()) {
            Thread.sleep(100);
        }
        Assert.assertEquals(count, values.size());
    }

    @Test
    public void testMultiplexingReader() throws Exception {
        redisWriter("multiplexing-reader-populate");
        RedisScanItemReader<String, String> scanReader = RedisScanItemReader.<String, String>builder().connection(client.connect()).build();
        RedisKeyspaceNotificationItemReader<String, String> notificationReader = RedisKeyspaceNotificationItemReader.<String, String>builder().connection(client.connectPubSub()).channel(RedisItemReader.getDefaultKeyEventChannel()).build();
        MultiplexingItemReader<String> compositeKeyReader = MultiplexingItemReader.<String>builder().readers(Arrays.asList(scanReader, notificationReader)).build();
        RedisValueReader<String, String> keyProcessor = RedisValueReader.<String, String>builder().pool(connectionPool).build();
        RedisItemReader<String, KeyValue<String>> reader = RedisItemReader.<String, KeyValue<String>>builder().keyReader(compositeKeyReader).valueReader(keyProcessor).threads(2).flushPeriod(Duration.ofMillis(50)).build();
        Map<String, KeyValue<String>> values = new HashMap<>();
        TaskletStep step = stepBuilderFactory.get("multiplexing-reader-step").<KeyValue<String>, KeyValue<String>>chunk(10).reader(reader).writer(items -> {
            for (KeyValue<String> item : items) {
                values.put(item.getKey(), item);
            }
        }).build();
        Job job = jobBuilderFactory.get("multiplexing-reader-job").start(step).build();
        JobExecution execution = asyncJobLauncher.run(job, new JobParameters());
        Thread.sleep(100);
        RedisCommands<String, String> commands = client.connect().sync();
        int count = 1233;
        for (int index = 0; index < count; index++) {
            commands.set("string:" + index, "value" + index);
            Thread.sleep(1);
        }
        Thread.sleep(300);
        notificationReader.stop();
        while (execution.isRunning()) {
            Thread.sleep(100);
        }
        Assert.assertEquals(INITIAL_SIZE + count, values.size());
    }

    private void populate(int iterations, RedisClient client) {
        Random random = new Random();
        RedisCommands<String, String> commands = client.connect().sync();
        for (int index = 0; index < iterations; index++) {
            String stringKey = "string:" + index;
            commands.set(stringKey, "value:" + index);
            commands.expire(stringKey, random.nextInt(1000));
            Map<String, String> hash = new HashMap<>();
            hash.put("field1", "value" + index);
            hash.put("field2", "value" + index);
            commands.hmset("hash:" + index, hash);
            commands.sadd("set:" + (index % 10), "member:" + index);
            commands.zadd("zset:" + (index % 10), index % 3, "member:" + index);
        }
    }

    @Test
    public void testReplication() throws Exception {
        populate(1039, client);
        RedisItemReader<String, KeyDump<String>> reader = RedisItemReader.dumpReaderBuilder().client(client).build();
        RedisItemWriter<String, String, KeyDump<String>> writer = RedisItemWriter.<String, String, KeyDump<String>>builder().command(Restore.<String, String>builder().build()).pool(targetConnectionPool).build();
        TaskletStep step = stepBuilderFactory.get("replication-step").<KeyDump<String>, KeyDump<String>>chunk(10).reader(reader).writer(writer).build();
        Job job = jobBuilderFactory.get("replication-job").start(step).build();
        jobLauncher.run(job, new JobParameters());
        compare("replication");
    }

    @Test
    public void testLiveReplication() throws Exception {
        populate(392, client);
        RedisItemReader<String, KeyDump<String>> reader = RedisItemReader.liveDumpReaderBuilder().client(client).build();
        RedisItemWriter<String, String, KeyDump<String>> writer = RedisItemWriter.<String, String, KeyDump<String>>builder().command(Restore.<String, String>builder().build()).pool(targetConnectionPool).build();
        TaskletStep step = stepBuilderFactory.get("live-replication-step").<KeyDump<String>, KeyDump<String>>chunk(10).reader(reader).writer(writer).build();
        Job job = jobBuilderFactory.get("live-replication-job").start(step).build();
        JobExecution execution = asyncJobLauncher.run(job, new JobParameters());
        AbstractScanItemReader<String> scanReader = (AbstractScanItemReader<String>) ((MultiplexingItemReader<String>) reader.getKeyReader()).getReaders().get(1);
        while (!scanReader.isDone()) {
            Thread.sleep(100);
        }
        ((RedisKeyspaceNotificationItemReader<String, String>) ((MultiplexingItemReader<String>) reader.getKeyReader()).getReaders().get(0)).stop();
        while (execution.isRunning()) {
            Thread.sleep(100);
        }
        compare("live-replication");
    }

    private void compare(String jobName) throws Exception {
        RedisCommands<String, String> sourceCommands = client.connect().sync();
        RedisCommands<String, String> targetCommands = targetClient.connect().sync();
        Assert.assertEquals(sourceCommands.dbsize(), targetCommands.dbsize());
        RedisScanItemReader<String, String> keyReader = RedisScanItemReader.<String, String>builder().connection(client.connect()).build();
        RedisDumpReader<String, String> sourceKeyReader = RedisDumpReader.<String, String>builder().pool(connectionPool).build();
        RedisDumpComparator<String, String> comparator = RedisDumpComparator.<String, String>builder().reader(sourceKeyReader).pool(targetConnectionPool).pttlTolerance(100L).build();
        RedisItemReader<String, KeyComparison<String>> reader = RedisItemReader.<String, KeyComparison<String>>builder().keyReader(keyReader).valueReader(comparator).build();
        TaskletStep step = stepBuilderFactory.get(jobName + "-step").<KeyComparison<String>, KeyComparison<String>>chunk(10).reader(reader).writer(l -> {
            for (KeyComparison<String> comparison : l) {
                Assertions.assertTrue(comparison.isOk());
                if (!comparison.isOk()) {
                    switch (comparison.getStatus()) {
                        case TTL:
                            long ttlDiff = comparison.getTarget().getPttl() - comparison.getSource().getPttl();
                            log.info(comparison.getSource().getKey() + ": " + ttlDiff);
                            break;
                        case MISSING:
                            log.info("Missing: {}", comparison.getSource().getKey());
                    }
                }
            }
        }).build();
        Job job = jobBuilderFactory.get(jobName + "-job").start(step).build();
        jobLauncher.run(job, new JobParameters());
    }

}