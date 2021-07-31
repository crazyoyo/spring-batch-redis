package org.springframework.batch.item.redis;

import com.redislabs.testcontainers.RedisClusterContainer;
import com.redislabs.testcontainers.RedisContainer;
import com.redislabs.testcontainers.RedisServer;
import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.sync.BaseRedisCommands;
import io.lettuce.core.api.sync.RedisServerCommands;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import io.lettuce.core.support.ConnectionPoolSupport;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.runner.RunWith;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.DefaultBufferedReaderFactory;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.separator.DefaultRecordSeparatorPolicy;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.redis.support.DataStructure;
import org.springframework.batch.item.redis.support.DataStructureValueReader;
import org.springframework.batch.item.redis.support.FlushingStepBuilder;
import org.springframework.batch.item.redis.support.KeyValue;
import org.springframework.batch.item.redis.support.KeyValueItemReader;
import org.springframework.batch.item.redis.support.LiveKeyValueItemReader;
import org.springframework.batch.item.redis.support.PollableItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.io.Resource;
import org.springframework.test.context.junit4.SpringRunner;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.BufferedReader;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

@Testcontainers
@SpringBootTest(classes = BatchTestApplication.class)
@RunWith(SpringRunner.class)
@SuppressWarnings({"unchecked", "unused", "BusyWait", "SingleStatementInBlock", "NullableProblems", "SameParameterValue"})
public class TestBase {

    @Container
    protected static final RedisContainer REDIS = new RedisContainer().withKeyspaceNotifications();
    @Container
    private static final RedisClusterContainer REDIS_CLUSTER = new RedisClusterContainer().withKeyspaceNotifications();

    static Stream<RedisServer> servers() {
        return Stream.of(REDIS, REDIS_CLUSTER);
    }

    @BeforeAll
    public static void setupRedisContainers() {
        add(REDIS, REDIS_CLUSTER);
    }

    protected static final Map<RedisServer, AbstractRedisClient> CLIENTS = new HashMap<>();
    protected static final Map<RedisServer, GenericObjectPool<? extends StatefulConnection<String, String>>> POOLS = new HashMap<>();
    protected static final Map<RedisServer, StatefulConnection<String, String>> CONNECTIONS = new HashMap<>();
    protected static final Map<RedisServer, StatefulRedisPubSubConnection<String, String>> PUBSUB_CONNECTIONS = new HashMap<>();
    protected static final Map<RedisServer, BaseRedisAsyncCommands<String, String>> ASYNCS = new HashMap<>();
    protected static final Map<RedisServer, BaseRedisCommands<String, String>> SYNCS = new HashMap<>();

    protected static void add(RedisServer... servers) {
        for (RedisServer server : servers) {
            if (server.isCluster()) {
                RedisClusterClient client = RedisClusterClient.create(server.getRedisURI());
                CLIENTS.put(server, client);
                StatefulRedisClusterConnection<String, String> connection = client.connect();
                CONNECTIONS.put(server, connection);
                SYNCS.put(server, connection.sync());
                ASYNCS.put(server, connection.async());
                PUBSUB_CONNECTIONS.put(server, client.connectPubSub());
                POOLS.put(server, ConnectionPoolSupport.createGenericObjectPool(client::connect, new GenericObjectPoolConfig<>()));
            } else {
                RedisClient client = RedisClient.create(server.getRedisURI());
                CLIENTS.put(server, client);
                StatefulRedisConnection<String, String> connection = client.connect();
                CONNECTIONS.put(server, connection);
                SYNCS.put(server, connection.sync());
                ASYNCS.put(server, connection.async());
                PUBSUB_CONNECTIONS.put(server, client.connectPubSub());
                POOLS.put(server, ConnectionPoolSupport.createGenericObjectPool(client::connect, new GenericObjectPoolConfig<>()));
            }
        }
    }

    @Autowired
    protected JobLauncher jobLauncher;
    @Autowired
    protected JobLauncher asyncJobLauncher;
    @Autowired
    protected JobBuilderFactory jobs;
    @Autowired
    protected StepBuilderFactory steps;

    @AfterEach
    public void flushall() {
        for (BaseRedisCommands<String, String> sync : SYNCS.values()) {
            ((RedisServerCommands<String, String>) sync).flushall();
        }
    }

    @AfterAll
    public static void teardown() {
        for (StatefulConnection<String, String> connection : CONNECTIONS.values()) {
            connection.close();
        }
        for (StatefulRedisPubSubConnection<String, String> pubSubConnection : PUBSUB_CONNECTIONS.values()) {
            pubSubConnection.close();
        }
        for (GenericObjectPool<? extends StatefulConnection<String, String>> pool : POOLS.values()) {
            pool.close();
        }
        for (AbstractRedisClient client : CLIENTS.values()) {
            client.shutdown();
            client.getResources().shutdown();
        }
        SYNCS.clear();
        ASYNCS.clear();
        CONNECTIONS.clear();
        PUBSUB_CONNECTIONS.clear();
        POOLS.clear();
        CLIENTS.clear();
    }

    protected static RedisClient redisClient(RedisServer redis) {
        return (RedisClient) CLIENTS.get(redis);
    }

    protected static RedisClusterClient redisClusterClient(RedisServer redis) {
        return (RedisClusterClient) CLIENTS.get(redis);
    }

    protected static <T> T sync(RedisServer server) {
        return (T) SYNCS.get(server);
    }

    protected static <T> T async(RedisServer server) {
        return (T) ASYNCS.get(server);
    }

    protected static <C extends StatefulConnection<String, String>> C connection(RedisServer server) {
        return (C) CONNECTIONS.get(server);
    }

    protected static <C extends StatefulRedisPubSubConnection<String, String>> C pubSubConnection(RedisServer server) {
        return (C) PUBSUB_CONNECTIONS.get(server);
    }

    protected static <C extends StatefulConnection<String, String>> GenericObjectPool<C> pool(RedisServer server) {
        if (POOLS.containsKey(server)) {
            return (GenericObjectPool<C>) POOLS.get(server);
        }
        throw new IllegalStateException("No pool found for " + server);
    }

    protected Job job(RedisServer redisServer, String name, TaskletStep step) {
        return jobs.get(name(redisServer, name) + "-job").start(step).build();
    }

    protected String name(RedisServer server, String name) {
        if (server.isCluster()) {
            return "cluster-" + name;
        }
        return name;
    }

    protected <I, O> JobExecution execute(RedisServer redisServer, String name, ItemReader<? extends I> reader, ItemWriter<O> writer) throws Throwable {
        return execute(redisServer, name, step(name, reader, writer).build());
    }

    protected JobExecution execute(RedisServer redisServer, String name, TaskletStep step) throws Exception {
        return checkForFailure(jobLauncher.run(job(redisServer, name, step), new JobParameters()));
    }

    protected JobExecution checkForFailure(JobExecution execution) {
        if (!execution.getExitStatus().getExitCode().equals(ExitStatus.COMPLETED.getExitCode())) {
            Assertions.fail("Job not completed: " + execution.getExitStatus());
        }
        return execution;
    }

    protected <I, O> JobExecution executeFlushing(RedisServer redisServer, String name, PollableItemReader<? extends I> reader, ItemWriter<O> writer) throws Throwable {
        TaskletStep step = flushing(step(name, reader, writer)).build();
        JobExecution execution = asyncJobLauncher.run(job(redisServer, name, step), new JobParameters());
        awaitRunning(execution);
        Thread.sleep(200);
        return execution;
    }

    protected <I, O> SimpleStepBuilder<I, O> step(String name, ItemReader<? extends I> reader, ItemWriter<O> writer) {
        return steps.get(name + "-step").<I, O>chunk(50).reader(reader).writer(writer);
    }

    protected <I, O> FlushingStepBuilder<I, O> flushing(SimpleStepBuilder<I, O> step) {
        return new FlushingStepBuilder<>(step).idleTimeout(Duration.ofMillis(500));
    }

    protected FlatFileItemReader<Map<String, String>> fileReader(Resource resource) throws IOException {
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

    protected void awaitJobTermination(JobExecution execution) throws Throwable {
        while (execution.isRunning()) {
            Thread.sleep(10);
        }
        checkForFailure(execution);
    }

    protected static class SynchronizedListItemWriter<T> implements ItemWriter<T> {

        private final List<T> writtenItems = Collections.synchronizedList(new ArrayList<>());

        @Override
        public void write(List<? extends T> items) {
            writtenItems.addAll(items);
        }

        public List<? extends T> getWrittenItems() {
            return this.writtenItems;
        }
    }

    protected void awaitRunning(JobExecution execution) throws InterruptedException {
        while (!execution.isRunning()) {
            Thread.sleep(10);
        }
    }

    protected DataGenerator.DataGeneratorBuilder dataGenerator(RedisServer server) {
        if (server.isCluster()) {
            return DataGenerator.client(redisClusterClient(server));
        }
        return DataGenerator.client(redisClient(server));
    }

    protected LiveKeyValueItemReader<KeyValue<byte[]>> liveKeyDumpReader(RedisServer server) {
        Duration idleTimeout = Duration.ofMillis(500);
        if (server.isCluster()) {
            return KeyDumpItemReader.client(redisClusterClient(server)).live().idleTimeout(idleTimeout).build();
        }
        return KeyDumpItemReader.client(redisClient(server)).live().idleTimeout(idleTimeout).build();
    }

    protected KeyValueItemReader<KeyValue<byte[]>> keyDumpReader(RedisServer server) {
        if (server.isCluster()) {
            return KeyDumpItemReader.client(redisClusterClient(server)).build();
        }
        return KeyDumpItemReader.client(redisClient(server)).build();
    }


    protected KeyDumpItemWriter keyDumpWriter(RedisServer redis) {
        if (redis.isCluster()) {
            return KeyDumpItemWriter.client(redisClusterClient(redis)).build();
        }
        return KeyDumpItemWriter.client(redisClient(redis)).build();
    }

    protected KeyValueItemReader<DataStructure> dataStructureReader(RedisServer server) {
        if (server.isCluster()) {
            return DataStructureItemReader.client(redisClusterClient(server)).build();
        }
        return DataStructureItemReader.client(redisClient(server)).build();
    }

    protected DataStructureValueReader dataStructureValueReader(RedisServer server) {
        if (server.isCluster()) {
            return DataStructureValueReader.client(redisClusterClient(server)).build();
        }
        return DataStructureValueReader.client(redisClient(server)).build();
    }

    protected DataStructureItemWriter<DataStructure> dataStructureWriter(RedisServer server) {
        if (server.isCluster()) {
            return DataStructureItemWriter.client(redisClusterClient(server)).build();
        }
        return DataStructureItemWriter.client(redisClient(server)).build();
    }

}
