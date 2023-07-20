package com.redis.spring.batch;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Method;
import java.time.Duration;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.function.Function;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.runner.RunWith;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionException;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.job.builder.SimpleJobBuilder;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemStream;
import org.springframework.batch.item.ItemStreamSupport;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.NonTransientResourceException;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.batch.support.transaction.ResourcelessTransactionManager;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.util.Assert;

import com.redis.lettucemod.RedisModulesClient;
import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.api.sync.RedisModulesCommands;
import com.redis.lettucemod.util.ClientBuilder;
import com.redis.lettucemod.util.RedisModulesUtils;
import com.redis.spring.batch.common.CompositeItemStreamProcessor;
import com.redis.spring.batch.common.KeyPredicateFactory;
import com.redis.spring.batch.common.KeyValue;
import com.redis.spring.batch.common.PredicateItemProcessor;
import com.redis.spring.batch.common.Utils;
import com.redis.spring.batch.convert.GeoValueConverter;
import com.redis.spring.batch.convert.ScoredValueConverter;
import com.redis.spring.batch.reader.GeneratorItemReader;
import com.redis.spring.batch.reader.GeneratorItemReader.StreamOptions;
import com.redis.spring.batch.reader.GeneratorItemReader.Type;
import com.redis.spring.batch.reader.KeyValueProcessor;
import com.redis.spring.batch.reader.KeyValueReadOperation;
import com.redis.spring.batch.reader.LiveRedisItemReader;
import com.redis.spring.batch.reader.PollableItemReader;
import com.redis.spring.batch.reader.ReaderOptions;
import com.redis.spring.batch.reader.ScanKeyItemReader;
import com.redis.spring.batch.reader.ScanOptions;
import com.redis.spring.batch.reader.ScanSizeEstimator;
import com.redis.spring.batch.reader.StreamAckPolicy;
import com.redis.spring.batch.reader.StreamItemReader;
import com.redis.spring.batch.reader.StructProcessor;
import com.redis.spring.batch.step.FlushingStepBuilder;
import com.redis.spring.batch.step.FlushingStepOptions;
import com.redis.spring.batch.writer.OperationItemWriter;
import com.redis.spring.batch.writer.ReplicaWaitOptions;
import com.redis.spring.batch.writer.StreamIdPolicy;
import com.redis.spring.batch.writer.WriteOperationOptions;
import com.redis.spring.batch.writer.WriterOptions;
import com.redis.spring.batch.writer.operation.Del;
import com.redis.spring.batch.writer.operation.Expire;
import com.redis.spring.batch.writer.operation.ExpireAt;
import com.redis.spring.batch.writer.operation.Geoadd;
import com.redis.spring.batch.writer.operation.Hset;
import com.redis.spring.batch.writer.operation.Lpush;
import com.redis.spring.batch.writer.operation.LpushAll;
import com.redis.spring.batch.writer.operation.Rpush;
import com.redis.spring.batch.writer.operation.Sadd;
import com.redis.spring.batch.writer.operation.Xadd;
import com.redis.spring.batch.writer.operation.Zadd;
import com.redis.testcontainers.RedisServer;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.Consumer;
import io.lettuce.core.GeoArgs;
import io.lettuce.core.KeyScanArgs;
import io.lettuce.core.Range;
import io.lettuce.core.RedisConnectionException;
import io.lettuce.core.RedisURI;
import io.lettuce.core.RestoreArgs;
import io.lettuce.core.ScanIterator;
import io.lettuce.core.ScoredValue;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.cluster.SlotHash;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.models.stream.PendingMessages;
import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.search.Search;
import io.micrometer.core.instrument.simple.SimpleConfig;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

@SpringBootTest(classes = BatchTestApplication.class)
@RunWith(SpringRunner.class)
@TestInstance(Lifecycle.PER_CLASS)
abstract class AbstractBatchTests {

    protected final Logger log = Logger.getLogger(getClass().getName());

    private static final Duration DEFAULT_AWAIT_TIMEOUT = Duration.ofMillis(1000);

    protected static final Duration DEFAULT_IDLE_TIMEOUT = Duration.ofMillis(300);

    private static final Duration DEFAULT_POLL_INTERVAL = Duration.ofMillis(30);

    protected static final int DEFAULT_GENERATOR_COUNT = 100;

    @Value("${running-timeout:PT5S}")
    private Duration runningTimeout;

    @Value("${termination-timeout:PT5S}")
    private Duration terminationTimeout;

    protected JobRepository jobRepository;

    protected AbstractRedisClient sourceClient;

    protected StatefulRedisModulesConnection<String, String> sourceConnection;

    private SimpleJobLauncher jobLauncher;

    private SimpleJobLauncher asyncJobLauncher;

    private JobBuilderFactory jobBuilderFactory;

    private StepBuilderFactory stepBuilderFactory;

    protected abstract RedisServer getSourceServer();

    @BeforeAll
    void setup() throws Exception {
        getSourceServer().start();
        sourceClient = client(getSourceServer());
        sourceConnection = RedisModulesUtils.connection(sourceClient);
        jobRepository = Utils.inMemoryJobRepository();
        jobLauncher = new SimpleJobLauncher();
        jobLauncher.setJobRepository(jobRepository);
        jobLauncher.afterPropertiesSet();
        asyncJobLauncher = new SimpleJobLauncher();
        asyncJobLauncher.setJobRepository(jobRepository);
        asyncJobLauncher.setTaskExecutor(new SimpleAsyncTaskExecutor());
        asyncJobLauncher.afterPropertiesSet();
        jobBuilderFactory = new JobBuilderFactory(jobRepository);
        stepBuilderFactory = new StepBuilderFactory(jobRepository, new ResourcelessTransactionManager());
    }

    @AfterAll
    void teardown() {
        sourceConnection.close();
        sourceClient.shutdown();
        sourceClient.getResources().shutdown();
        getSourceServer().close();
    }

    @BeforeEach
    void flushAll() {
        sourceConnection.sync().flushall();
    }

    private static class SimpleTestInfo implements TestInfo {

        private final TestInfo delegate;

        private final String[] suffixes;

        public SimpleTestInfo(TestInfo delegate, String... suffixes) {
            this.delegate = delegate;
            this.suffixes = suffixes;
        }

        @Override
        public String getDisplayName() {
            List<String> elements = new ArrayList<>();
            elements.add(delegate.getDisplayName());
            elements.addAll(Arrays.asList(suffixes));
            return String.join("-", elements);
        }

        @Override
        public Set<String> getTags() {
            return delegate.getTags();
        }

        @Override
        public Optional<Class<?>> getTestClass() {
            return delegate.getTestClass();
        }

        @Override
        public Optional<Method> getTestMethod() {
            return delegate.getTestMethod();
        }

    }

    protected <T> List<T> readAllAndClose(TestInfo testInfo, ItemReader<T> reader)
            throws UnexpectedInputException, ParseException, NonTransientResourceException, Exception {
        try {
            return readAll(testInfo, reader);
        } finally {
            if (reader instanceof ItemStream) {
                ((ItemStream) reader).close();
            }
        }
    }

    protected <T> List<T> readAll(TestInfo testInfo, ItemReader<T> reader)
            throws UnexpectedInputException, ParseException, NonTransientResourceException, Exception {
        if (reader instanceof ItemStream) {
            if (reader instanceof ItemStreamSupport) {
                ((ItemStreamSupport) reader).setName(name(testInfo) + "-readAll");
            }
            ((ItemStream) reader).open(new ExecutionContext());
        }
        return Utils.readAll(reader);
    }

    protected static void awaitClosed(Object object) {
        if (object instanceof ItemStream) {
            awaitUntilFalse(() -> Utils.isOpen((ItemStream) object, false));
        }
    }

    protected <I, O> SimpleStepBuilder<I, O> step(TestInfo testInfo, ItemReader<I> reader, ItemWriter<O> writer) {
        String name = name(testInfo);
        if (reader instanceof ItemStreamSupport) {
            ((ItemStreamSupport) reader).setName(name + "-reader");
        }
        SimpleStepBuilder<I, O> step = stepBuilderFactory.get(name).chunk(ReaderOptions.DEFAULT_CHUNK_SIZE);
        step.reader(reader);
        step.writer(writer);
        if (reader instanceof PollableItemReader) {
            return new FlushingStepBuilder<>(step)
                    .options(FlushingStepOptions.builder().idleTimeout(DEFAULT_IDLE_TIMEOUT).build());
        }
        return step;
    }

    public static String name(TestInfo testInfo) {
        return testInfo.getDisplayName().replace("(TestInfo)", "");
    }

    public static TestInfo testInfo(TestInfo testInfo, String... suffixes) {
        return new SimpleTestInfo(testInfo, suffixes);
    }

    protected AbstractRedisClient client(RedisServer server) {
        RedisURI uri = RedisURI.create(server.getRedisURI());
        return ClientBuilder.create(uri).cluster(server.isCluster()).build();
    }

    public static JobExecution awaitRunning(JobExecution jobExecution) {
        Awaitility.await().until(() -> isRunning(jobExecution));
        return jobExecution;
    }

    public static JobExecution awaitTermination(JobExecution jobExecution) {
        Awaitility.await().until(() -> isTerminated(jobExecution));
        return jobExecution;
    }

    public static boolean isRunning(JobExecution jobExecution) {
        return jobExecution.isRunning() || jobExecution.getStatus().isUnsuccessful()
                || jobExecution.getStatus() != BatchStatus.STARTING;
    }

    public static boolean isTerminated(JobExecution jobExecution) {
        return !jobExecution.isRunning() || jobExecution.getStatus().isUnsuccessful()
                || jobExecution.getStatus() == BatchStatus.COMPLETED || jobExecution.getStatus() == BatchStatus.STOPPED
                || jobExecution.getStatus().isGreaterThan(BatchStatus.STOPPED);
    }

    protected static void awaitUntilFalse(Callable<Boolean> conditionEvaluator) {
        awaitUntil(() -> !conditionEvaluator.call());
    }

    protected static void awaitUntil(Callable<Boolean> conditionEvaluator) {
        Awaitility.await().pollInterval(DEFAULT_POLL_INTERVAL).timeout(DEFAULT_AWAIT_TIMEOUT).until(conditionEvaluator);
    }

    protected JobBuilder job(TestInfo testInfo) {
        return jobBuilderFactory.get(name(testInfo));
    }

    protected void generate(TestInfo testInfo) throws JobExecutionException {
        GeneratorItemReader gen = new GeneratorItemReader();
        gen.setMaxItemCount(DEFAULT_GENERATOR_COUNT);
        generate(testInfo, gen);
    }

    protected void generate(TestInfo testInfo, GeneratorItemReader reader) throws JobExecutionException {
        generate(testInfo, sourceClient, reader);
    }

    protected void generate(TestInfo testInfo, AbstractRedisClient client, GeneratorItemReader reader)
            throws JobExecutionException {
        TestInfo finalTestInfo = testInfo(testInfo, "generate", String.valueOf(client.hashCode()));
        RedisItemWriter<String, String> writer = writer(client)
                .writerOptions(WriterOptions.builder().streamIdPolicy(StreamIdPolicy.DROP).build()).struct();
        run(finalTestInfo, reader, writer);
    }

    protected RedisItemWriter.Builder<String, String> writer(AbstractRedisClient client) {
        return writer(client, StringCodec.UTF8);
    }

    protected <K, V> RedisItemWriter.Builder<K, V> writer(AbstractRedisClient client, RedisCodec<K, V> codec) {
        return new RedisItemWriter.Builder<>(client, codec);
    }

    protected void configure(LiveRedisItemReader<?, ?> builder) {
        builder.setFlushingOptions(FlushingStepOptions.builder().idleTimeout(DEFAULT_IDLE_TIMEOUT).build());
    }

    protected RedisItemReader.Builder<String, String> reader(AbstractRedisClient client) {
        return reader(client, StringCodec.UTF8);
    }

    protected <K, V> RedisItemReader.Builder<K, V> reader(AbstractRedisClient client, RedisCodec<K, V> codec) {
        return RedisItemReader.client(client, codec).jobRepository(jobRepository);
    }

    protected void flushAll(AbstractRedisClient client) {
        try (StatefulRedisModulesConnection<String, String> connection = RedisModulesUtils.connection(client)) {
            connection.sync().flushall();
            awaitUntil(() -> connection.sync().dbsize() == 0);
        }
    }

    protected <I, O> JobExecution run(TestInfo testInfo, ItemReader<I> reader, ItemWriter<O> writer)
            throws JobExecutionException {
        SimpleStepBuilder<I, O> step = step(testInfo, reader, writer);
        SimpleJobBuilder job = job(testInfo).start(step.build());
        JobExecution execution = run(job.build());
        awaitClosed(reader);
        awaitClosed(writer);
        return execution;
    }

    protected JobExecution run(Job job) throws JobExecutionException {
        return jobLauncher.run(job, new JobParameters());
    }

    protected JobExecution runAsync(Job job) throws JobExecutionException {
        JobExecution execution = asyncJobLauncher.run(job, new JobParameters());
        awaitRunning(execution);
        return execution;
    }

    protected void enableKeyspaceNotifications(AbstractRedisClient client) {
        RedisModulesUtils.connection(client).sync().configSet("notify-keyspace-events", "AK");
    }

    @Test
    void writeWait(TestInfo testInfo) throws Exception {
        List<Map<String, String>> maps = new ArrayList<>();
        for (int index = 0; index < 100; index++) {
            Map<String, String> body = new HashMap<>();
            body.put("id", String.valueOf(index));
            body.put("field1", "value1");
            body.put("field2", "value2");
            maps.add(body);
        }
        ListItemReader<Map<String, String>> reader = new ListItemReader<>(maps);
        Hset<String, String, Map<String, String>> hset = new Hset<>(m -> "hash:" + m.remove("id"), Function.identity());
        OperationItemWriter<String, String, Map<String, String>> writer = new OperationItemWriter<>(sourceClient,
                StringCodec.UTF8, hset);
        ReplicaWaitOptions waitOptions = ReplicaWaitOptions.builder().replicas(1).timeout(Duration.ofMillis(300)).build();
        writer.setOptions(WriteOperationOptions.builder().replicaWaitOptions(waitOptions).build());
        JobExecution execution = run(testInfo, reader, writer);
        List<Throwable> exceptions = execution.getAllFailureExceptions();
        assertEquals("Insufficient replication level (0/1)", exceptions.get(0).getCause().getMessage());
    }

    protected <I, O> Job job(TestInfo testInfo, ItemReader<I> reader, ItemWriter<O> writer) {
        return job(testInfo).start(step(testInfo, reader, writer).build()).build();
    }

    @Test
    void writeHash(TestInfo testInfo) throws Exception {
        List<Map<String, String>> maps = new ArrayList<>();
        for (int index = 0; index < 100; index++) {
            Map<String, String> body = new HashMap<>();
            body.put("id", String.valueOf(index));
            body.put("field1", "value1");
            body.put("field2", "value2");
            maps.add(body);
        }
        ListItemReader<Map<String, String>> reader = new ListItemReader<>(maps);
        Hset<String, String, Map<String, String>> hset = new Hset<>(m -> "hash:" + m.remove("id"), Function.identity());
        OperationItemWriter<String, String, Map<String, String>> writer = new OperationItemWriter<>(sourceClient,
                StringCodec.UTF8, hset);
        run(testInfo, reader, writer);
        assertEquals(maps.size(), sourceConnection.sync().keys("hash:*").size());
        for (int index = 0; index < maps.size(); index++) {
            Map<String, String> hash = sourceConnection.sync().hgetall("hash:" + index);
            assertEquals(maps.get(index), hash);
        }
    }

    @Test
    void writeDel(TestInfo testInfo) throws Exception {
        generate(testInfo);
        GeneratorItemReader gen = new GeneratorItemReader();
        gen.setMaxItemCount(DEFAULT_GENERATOR_COUNT);
        Del<String, String, KeyValue<String>> del = new Del<>(KeyValue::getKey);
        run(testInfo, gen, new OperationItemWriter<>(sourceClient, StringCodec.UTF8, del));
        assertEquals(0, sourceConnection.sync().keys(GeneratorItemReader.DEFAULT_KEYSPACE + "*").size());
    }

    @Test
    void writeLpush(TestInfo testInfo) throws Exception {
        GeneratorItemReader gen = new GeneratorItemReader();
        gen.setMaxItemCount(DEFAULT_GENERATOR_COUNT);
        gen.setTypes(Type.STRING);
        Lpush<String, String, KeyValue<String>> lpush = new Lpush<>(KeyValue::getKey, v -> (String) v.getValue());
        run(testInfo, gen, new OperationItemWriter<>(sourceClient, StringCodec.UTF8, lpush));
        assertEquals(DEFAULT_GENERATOR_COUNT, sourceConnection.sync().dbsize());
        for (String key : sourceConnection.sync().keys("*")) {
            assertEquals(KeyValue.LIST, sourceConnection.sync().type(key));
        }
    }

    @Test
    void writeRpush(TestInfo testInfo) throws Exception {
        GeneratorItemReader gen = new GeneratorItemReader();
        gen.setMaxItemCount(DEFAULT_GENERATOR_COUNT);
        gen.setTypes(Type.STRING);
        Rpush<String, String, KeyValue<String>> rpush = new Rpush<>(KeyValue::getKey, v -> (String) v.getValue());
        run(testInfo, gen, new OperationItemWriter<>(sourceClient, StringCodec.UTF8, rpush));
        assertEquals(DEFAULT_GENERATOR_COUNT, sourceConnection.sync().dbsize());
        for (String key : sourceConnection.sync().keys("*")) {
            assertEquals(KeyValue.LIST, sourceConnection.sync().type(key));
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    void writeLpushAll(TestInfo testInfo) throws Exception {
        GeneratorItemReader gen = new GeneratorItemReader();
        gen.setMaxItemCount(DEFAULT_GENERATOR_COUNT);
        gen.setTypes(Type.LIST);
        LpushAll<String, String, KeyValue<String>> lpushAll = new LpushAll<>(KeyValue::getKey,
                v -> (Collection<String>) v.getValue());
        run(testInfo, gen, new OperationItemWriter<>(sourceClient, StringCodec.UTF8, lpushAll));
        assertEquals(DEFAULT_GENERATOR_COUNT, sourceConnection.sync().dbsize());
        for (String key : sourceConnection.sync().keys("*")) {
            assertEquals(KeyValue.LIST, sourceConnection.sync().type(key));
        }
    }

    @Test
    void writeExpire(TestInfo testInfo) throws Exception {
        GeneratorItemReader gen = new GeneratorItemReader();
        gen.setMaxItemCount(DEFAULT_GENERATOR_COUNT);
        gen.setTypes(Type.STRING);
        Expire<String, String, KeyValue<String>> expire = new Expire<>(KeyValue::getKey, v -> 1L);
        run(testInfo, gen, new OperationItemWriter<>(sourceClient, StringCodec.UTF8, expire));
        awaitUntil(() -> sourceConnection.sync().keys("*").isEmpty());
        assertEquals(0, sourceConnection.sync().dbsize());
    }

    @Test
    void writeExpireAt(TestInfo testInfo) throws Exception {
        GeneratorItemReader gen = new GeneratorItemReader();
        gen.setMaxItemCount(DEFAULT_GENERATOR_COUNT);
        gen.setTypes(Type.STRING);
        ExpireAt<String, String, KeyValue<String>> expireAt = new ExpireAt<>(KeyValue::getKey, v -> System.currentTimeMillis());
        run(testInfo, gen, new OperationItemWriter<>(sourceClient, StringCodec.UTF8, expireAt));
        awaitUntil(() -> sourceConnection.sync().keys("*").isEmpty());
        assertEquals(0, sourceConnection.sync().dbsize());
    }

    private class Geo {

        private String member;

        private double longitude;

        private double latitude;

        public Geo(String member, double longitude, double latitude) {
            this.member = member;
            this.longitude = longitude;
            this.latitude = latitude;
        }

        public String getMember() {
            return member;
        }

        public double getLongitude() {
            return longitude;
        }

        public double getLatitude() {
            return latitude;
        }

    }

    @Test
    void writeGeo(TestInfo testInfo) throws Exception {
        ListItemReader<Geo> reader = new ListItemReader<>(Arrays.asList(new Geo("Venice Breakwater", -118.476056, 33.985728),
                new Geo("Long Beach National", -73.667022, 40.582739)));
        GeoValueConverter<String, Geo> value = new GeoValueConverter<>(Geo::getMember, Geo::getLongitude, Geo::getLatitude);
        Geoadd<String, String, Geo> geoadd = new Geoadd<>(t -> "geoset", value);
        OperationItemWriter<String, String, Geo> writer = new OperationItemWriter<>(sourceClient, StringCodec.UTF8, geoadd);
        run(testInfo, reader, writer);
        Set<String> radius1 = sourceConnection.sync().georadius("geoset", -118, 34, 100, GeoArgs.Unit.mi);
        assertEquals(1, radius1.size());
        assertTrue(radius1.contains("Venice Breakwater"));
    }

    @Test
    void writeHashDel(TestInfo testInfo) throws Exception {
        List<Entry<String, Map<String, String>>> hashes = new ArrayList<>();
        RedisModulesCommands<String, String> sync = sourceConnection.sync();
        for (int index = 0; index < 100; index++) {
            String key = String.valueOf(index);
            Map<String, String> value = new HashMap<>();
            value.put("field1", "value1");
            sync.hset("hash:" + key, value);
            Map<String, String> body = new HashMap<>();
            body.put("field2", "value2");
            hashes.add(new AbstractMap.SimpleEntry<>(key, index < 50 ? null : body));
        }
        ListItemReader<Map.Entry<String, Map<String, String>>> reader = new ListItemReader<>(hashes);
        Hset<String, String, Entry<String, Map<String, String>>> hset = new Hset<>(e -> "hash:" + e.getKey(), Entry::getValue);
        OperationItemWriter<String, String, Entry<String, Map<String, String>>> writer = new OperationItemWriter<>(sourceClient,
                StringCodec.UTF8, hset);
        run(testInfo, reader, writer);
        assertEquals(100, sync.keys("hash:*").size());
        assertEquals(2, sync.hgetall("hash:50").size());
    }

    private class ZValue {

        private String member;

        private double score;

        public ZValue(String member, double score) {
            super();
            this.member = member;
            this.score = score;
        }

        public String getMember() {
            return member;
        }

        public double getScore() {
            return score;
        }

    }

    @Test
    void writeZset(TestInfo testInfo) throws Exception {
        String key = "zadd";
        List<ZValue> values = new ArrayList<>();
        for (int index = 0; index < 100; index++) {
            values.add(new ZValue(String.valueOf(index), index % 10));
        }
        ListItemReader<ZValue> reader = new ListItemReader<>(values);
        ScoredValueConverter<String, ZValue> converter = new ScoredValueConverter<>(ZValue::getMember, ZValue::getScore);
        Zadd<String, String, ZValue> zadd = new Zadd<>(t -> key, converter);
        OperationItemWriter<String, String, ZValue> writer = new OperationItemWriter<>(sourceClient, StringCodec.UTF8, zadd);
        run(testInfo, reader, writer);
        RedisModulesCommands<String, String> sync = sourceConnection.sync();
        assertEquals(1, sync.dbsize());
        assertEquals(values.size(), sync.zcard(key));
        assertEquals(60, sync.zrangebyscore(key, Range.from(Range.Boundary.including(0), Range.Boundary.including(5))).size());
    }

    @Test
    void writeSet(TestInfo testInfo) throws Exception {
        String key = "sadd";
        List<String> values = new ArrayList<>();
        for (int index = 0; index < 100; index++) {
            values.add(String.valueOf(index));
        }
        ListItemReader<String> reader = new ListItemReader<>(values);
        Sadd<String, String, String> sadd = new Sadd<>(t -> key, Function.identity());
        OperationItemWriter<String, String, String> writer = new OperationItemWriter<>(sourceClient, StringCodec.UTF8, sadd);
        run(testInfo, reader, writer);
        RedisModulesCommands<String, String> sync = sourceConnection.sync();
        assertEquals(1, sync.dbsize());
        assertEquals(values.size(), sync.scard(key));
    }

    @Test
    void writeStructs(TestInfo testInfo) throws Exception {
        List<KeyValue<String>> list = new ArrayList<>();
        long count = 100;
        for (int index = 0; index < count; index++) {
            Map<String, String> map = new HashMap<>();
            map.put("field1", "value1");
            map.put("field2", "value2");
            KeyValue<String> ds = new KeyValue<>();
            ds.setKey("hash:" + index);
            ds.setType(KeyValue.HASH);
            ds.setValue(map);
            list.add(ds);
        }
        ListItemReader<KeyValue<String>> reader = new ListItemReader<>(list);
        RedisItemWriter<String, String> writer = writer(sourceClient).struct();
        run(testInfo, reader, writer);
        RedisModulesCommands<String, String> sync = sourceConnection.sync();
        List<String> keys = sync.keys("hash:*");
        assertEquals(count, keys.size());
    }

    @Test
    void metrics(TestInfo testInfo) throws Exception {
        Metrics.globalRegistry.getMeters().forEach(Metrics.globalRegistry::remove);
        SimpleMeterRegistry registry = new SimpleMeterRegistry(new SimpleConfig() {

            @Override
            public String get(String key) {
                return null;
            }

            @Override
            public Duration step() {
                return Duration.ofMillis(1);
            }

        }, Clock.SYSTEM);
        Metrics.addRegistry(registry);
        generate(testInfo);
        RedisItemReader<String, String> reader = reader(sourceClient).struct();
        open(reader);
        Search search = registry.find("spring.batch.redis.reader.queue.size");
        Assertions.assertNotNull(search.gauge());
        reader.close();
        registry.close();
        Metrics.globalRegistry.getMeters().forEach(Metrics.globalRegistry::remove);
    }

    @Test
    void filterKeySlot(TestInfo testInfo) throws Exception {
        enableKeyspaceNotifications(sourceClient);
        LiveRedisItemReader<String, String> reader = reader(sourceClient).live().struct();
        reader.setKeyProcessor(PredicateItemProcessor.of(KeyPredicateFactory.create().slotRange(0, 8000).build()));
        SynchronizedListItemWriter<KeyValue<String>> writer = new SynchronizedListItemWriter<>();
        JobExecution execution = runAsync(job(testInfo, reader, writer));
        int count = 100;
        GeneratorItemReader gen = new GeneratorItemReader();
        gen.setMaxItemCount(count);
        generate(testInfo, gen);
        awaitTermination(execution);
        Assertions.assertFalse(
                writer.getItems().stream().map(KeyValue::getKey).map(SlotHash::getSlot).anyMatch(s -> s < 0 || s > 8000));
    }

    @Test
    void scanKeyItemReader(TestInfo testInfo)
            throws UnexpectedInputException, ParseException, NonTransientResourceException, Exception {
        generate(testInfo);
        ScanKeyItemReader<String, String> reader = new ScanKeyItemReader<>(sourceClient, StringCodec.UTF8);
        List<String> keys = readAllAndClose(testInfo, reader);
        Assertions.assertEquals(sourceConnection.sync().dbsize(), keys.size());
    }

    @Test
    void reader(TestInfo testInfo) throws Exception {
        generate(testInfo);
        RedisItemReader<String, String> reader = reader(sourceClient).struct();
        List<KeyValue<String>> list = readAllAndClose(testInfo, reader);
        assertEquals(sourceConnection.sync().dbsize(), list.size());
    }

    @Test
    void readThreads(TestInfo testInfo) throws Exception {
        generate(testInfo);
        RedisItemReader<String, String> reader = reader(sourceClient).struct();
        SynchronizedListItemWriter<KeyValue<String>> writer = new SynchronizedListItemWriter<>();
        int threads = 4;
        SimpleStepBuilder<KeyValue<String>, KeyValue<String>> step = step(testInfo, reader, writer);
        Utils.multiThread(step, threads);
        run(job(testInfo).start(step.build()).build());
        awaitClosed(reader);
        awaitClosed(writer);
        assertEquals(sourceConnection.sync().dbsize(),
                writer.getItems().stream().collect(Collectors.toMap(KeyValue::getKey, t -> t)).size());
    }

    @Test
    void scanSizeEstimator(TestInfo testInfo) throws Exception {
        GeneratorItemReader gen = new GeneratorItemReader();
        gen.setMaxItemCount(12345);
        gen.setTypes(Type.HASH, Type.STRING);
        generate(testInfo, gen);
        long expectedCount = sourceConnection.sync().dbsize();
        ScanSizeEstimator estimator = new ScanSizeEstimator(sourceClient);
        assertEquals(expectedCount,
                estimator.estimateSize(ScanOptions.builder().match(GeneratorItemReader.DEFAULT_KEYSPACE + "*").build()),
                expectedCount / 10);
        estimator = new ScanSizeEstimator(sourceClient);
        assertEquals(expectedCount / 2, estimator.estimateSize(ScanOptions.builder().type(KeyValue.HASH).build()),
                expectedCount / 10);
    }

    protected void generateStreams(TestInfo testInfo, int messageCount) throws JobExecutionException {
        GeneratorItemReader gen = new GeneratorItemReader();
        gen.setTypes(Arrays.asList(Type.STREAM));
        gen.setMaxItemCount(3);
        gen.setStreamOptions(StreamOptions.builder().messageCount(messageCount).build());
        generate(testInfo(testInfo, "streams"), gen);
    }

    protected StreamItemReader<String, String> streamReader(String stream, Consumer<String> consumer) {
        return new StreamItemReader<>(sourceClient, StringCodec.UTF8, stream, consumer);
    }

    protected void assertMessageBody(List<? extends StreamMessage<String, String>> items) {
        for (StreamMessage<String, String> message : items) {
            assertTrue(message.getBody().containsKey("field1"));
            assertTrue(message.getBody().containsKey("field2"));
        }
    }

    private void assertStreamEquals(String expectedId, Map<String, String> expectedBody, String expectedStream,
            StreamMessage<String, String> message) {
        Assertions.assertEquals(expectedId, message.getId());
        Assertions.assertEquals(expectedBody, message.getBody());
        Assertions.assertEquals(expectedStream, message.getStream());
    }

    private Map<String, String> map(String... args) {
        Assert.notNull(args, "Args cannot be null");
        Assert.isTrue(args.length % 2 == 0, "Args length is not a multiple of 2");
        Map<String, String> body = new LinkedHashMap<>();
        for (int index = 0; index < args.length / 2; index++) {
            body.put(args[index * 2], args[index * 2 + 1]);
        }
        return body;
    }

    @Test
    void writeStream(TestInfo testInfo) throws Exception {
        String stream = "stream:0";
        List<Map<String, String>> messages = new ArrayList<>();
        for (int index = 0; index < 100; index++) {
            Map<String, String> body = new HashMap<>();
            body.put("field1", "value1");
            body.put("field2", "value2");
            messages.add(body);
        }
        ListItemReader<Map<String, String>> reader = new ListItemReader<>(messages);
        Xadd<String, String, Map<String, String>> xadd = new Xadd<>(t -> stream, Function.identity(), m -> null);
        OperationItemWriter<String, String, Map<String, String>> writer = new OperationItemWriter<>(sourceClient,
                StringCodec.UTF8, xadd);
        run(testInfo, reader, writer);
        RedisModulesCommands<String, String> sync = sourceConnection.sync();
        Assertions.assertEquals(messages.size(), sync.xlen(stream));
        List<StreamMessage<String, String>> xrange = sync.xrange(stream, Range.create("-", "+"));
        for (int index = 0; index < xrange.size(); index++) {
            StreamMessage<String, String> message = xrange.get(index);
            Assertions.assertEquals(messages.get(index), message.getBody());
        }
    }

    @Test
    void readStreamAutoAck() throws InterruptedException {
        String stream = "stream1";
        String consumerGroup = "batchtests-readStreamAutoAck";
        Consumer<String> consumer = Consumer.from(consumerGroup, "consumer1");
        final StreamItemReader<String, String> reader = streamReader(stream, consumer);
        reader.setAckPolicy(StreamAckPolicy.AUTO);
        open(reader);
        String field1 = "field1";
        String value1 = "value1";
        String field2 = "field2";
        String value2 = "value2";
        Map<String, String> body = map(field1, value1, field2, value2);
        String id1 = sourceConnection.sync().xadd(stream, body);
        String id2 = sourceConnection.sync().xadd(stream, body);
        String id3 = sourceConnection.sync().xadd(stream, body);
        List<StreamMessage<String, String>> messages = new ArrayList<>();
        awaitUntil(() -> messages.addAll(reader.readMessages()));
        Assertions.assertEquals(3, messages.size());
        assertStreamEquals(id1, body, stream, messages.get(0));
        assertStreamEquals(id2, body, stream, messages.get(1));
        assertStreamEquals(id3, body, stream, messages.get(2));
        reader.close();
        Assertions.assertEquals(0, sourceConnection.sync().xpending(stream, consumerGroup).getCount(), "pending messages");
    }

    @Test
    void readStreamManualAck() throws Exception {
        String stream = "stream1";
        String consumerGroup = "batchtests-readStreamManualAck";
        Consumer<String> consumer = Consumer.from(consumerGroup, "consumer1");
        final StreamItemReader<String, String> reader = streamReader(stream, consumer);
        reader.setAckPolicy(StreamAckPolicy.MANUAL);
        open(reader);
        String field1 = "field1";
        String value1 = "value1";
        String field2 = "field2";
        String value2 = "value2";
        Map<String, String> body = map(field1, value1, field2, value2);
        String id1 = sourceConnection.sync().xadd(stream, body);
        String id2 = sourceConnection.sync().xadd(stream, body);
        String id3 = sourceConnection.sync().xadd(stream, body);
        List<StreamMessage<String, String>> messages = new ArrayList<>();
        awaitUntil(() -> messages.addAll(reader.readMessages()));
        Assertions.assertEquals(3, messages.size());

        assertStreamEquals(id1, body, stream, messages.get(0));
        assertStreamEquals(id2, body, stream, messages.get(1));
        assertStreamEquals(id3, body, stream, messages.get(2));
        PendingMessages pendingMsgsBeforeCommit = sourceConnection.sync().xpending(stream, consumerGroup);
        Assertions.assertEquals(3, pendingMsgsBeforeCommit.getCount(), "pending messages before commit");
        sourceConnection.sync().xack(stream, consumerGroup, messages.get(0).getId(), messages.get(1).getId());
        PendingMessages pendingMsgsAfterCommit = sourceConnection.sync().xpending(stream, consumerGroup);
        Assertions.assertEquals(1, pendingMsgsAfterCommit.getCount(), "pending messages after commit");
        reader.close();
    }

    @Test
    void readStreamManualAckRecover() throws InterruptedException {
        String stream = "stream1";
        Consumer<String> consumer = Consumer.from("batchtests-readStreamManualAckRecover", "consumer1");
        final StreamItemReader<String, String> reader = streamReader(stream, consumer);
        reader.setAckPolicy(StreamAckPolicy.MANUAL);
        open(reader);
        String field1 = "field1";
        String value1 = "value1";
        String field2 = "field2";
        String value2 = "value2";
        Map<String, String> body = map(field1, value1, field2, value2);
        sourceConnection.sync().xadd(stream, body);
        sourceConnection.sync().xadd(stream, body);
        sourceConnection.sync().xadd(stream, body);
        List<StreamMessage<String, String>> messages = new ArrayList<>();
        awaitUntil(() -> messages.addAll(reader.readMessages()));
        Assertions.assertEquals(3, messages.size());

        List<StreamMessage<String, String>> recoveredMessages = new ArrayList<>();
        sourceConnection.sync().xadd(stream, body);
        sourceConnection.sync().xadd(stream, body);
        sourceConnection.sync().xadd(stream, body);

        reader.close();

        final StreamItemReader<String, String> reader2 = streamReader(stream, consumer);
        reader2.setAckPolicy(StreamAckPolicy.MANUAL);
        open(reader2);

        awaitUntil(() -> recoveredMessages.addAll(reader2.readMessages()));
        awaitUntil(() -> !recoveredMessages.addAll(reader2.readMessages()));

        Assertions.assertEquals(6, recoveredMessages.size());
    }

    @Test
    void readStreamManualAckRecoverUncommitted() throws InterruptedException {
        String stream = "stream1";
        String consumerGroup = "batchtests-readStreamManualAckRecoverUncommitted";
        Consumer<String> consumer = Consumer.from(consumerGroup, "consumer1");
        final StreamItemReader<String, String> reader = streamReader(stream, consumer);
        reader.setAckPolicy(StreamAckPolicy.MANUAL);
        open(reader);
        String field1 = "field1";
        String value1 = "value1";
        String field2 = "field2";
        String value2 = "value2";
        Map<String, String> body = map(field1, value1, field2, value2);
        sourceConnection.sync().xadd(stream, body);
        sourceConnection.sync().xadd(stream, body);
        String id3 = sourceConnection.sync().xadd(stream, body);
        List<StreamMessage<String, String>> messages = new ArrayList<>();
        awaitUntil(() -> messages.addAll(reader.readMessages()));
        Assertions.assertEquals(3, messages.size());
        sourceConnection.sync().xack(stream, consumerGroup, messages.get(0).getId(), messages.get(1).getId());

        List<StreamMessage<String, String>> recoveredMessages = new ArrayList<>();
        String id4 = sourceConnection.sync().xadd(stream, body);
        String id5 = sourceConnection.sync().xadd(stream, body);
        String id6 = sourceConnection.sync().xadd(stream, body);
        reader.close();

        final StreamItemReader<String, String> reader2 = streamReader(stream, consumer);
        reader2.setAckPolicy(StreamAckPolicy.MANUAL);
        reader2.setOffset(messages.get(1).getId());
        open(reader2);

        // Wait until task.poll() doesn't return any more records
        awaitUntil(() -> recoveredMessages.addAll(reader2.readMessages()));
        awaitUntil(() -> !recoveredMessages.addAll(reader2.readMessages()));
        List<String> recoveredIds = recoveredMessages.stream().map(StreamMessage::getId).collect(Collectors.toList());
        Assertions.assertEquals(Arrays.<String> asList(id3, id4, id5, id6), recoveredIds, "recoveredIds");
        reader2.close();
    }

    @Test
    void readStreamManualAckRecoverFromOffset() throws Exception {
        String stream = "stream1";
        String consumerGroup = "batchtests-readStreamManualAckRecoverFromOffset";
        Consumer<String> consumer = Consumer.from(consumerGroup, "consumer1");
        final StreamItemReader<String, String> reader = streamReader(stream, consumer);
        reader.setAckPolicy(StreamAckPolicy.MANUAL);
        open(reader);
        String field1 = "field1";
        String value1 = "value1";
        String field2 = "field2";
        String value2 = "value2";
        Map<String, String> body = map(field1, value1, field2, value2);
        sourceConnection.sync().xadd(stream, body);
        sourceConnection.sync().xadd(stream, body);
        String id3 = sourceConnection.sync().xadd(stream, body);
        List<StreamMessage<String, String>> sourceRecords = new ArrayList<>();
        awaitUntil(() -> sourceRecords.addAll(reader.readMessages()));
        Assertions.assertEquals(3, sourceRecords.size());

        List<StreamMessage<String, String>> recoveredRecords = new ArrayList<>();
        String id4 = sourceConnection.sync().xadd(stream, body);
        String id5 = sourceConnection.sync().xadd(stream, body);
        String id6 = sourceConnection.sync().xadd(stream, body);

        reader.close();

        final StreamItemReader<String, String> reader2 = streamReader(stream, consumer);
        reader2.setAckPolicy(StreamAckPolicy.MANUAL);
        reader2.setOffset(id3);
        open(reader2);

        // Wait until task.poll() doesn't return any more records
        awaitUntil(() -> recoveredRecords.addAll(reader2.readMessages()));
        awaitUntil(() -> !recoveredRecords.addAll(reader2.readMessages()));
        List<String> recoveredIds = recoveredRecords.stream().map(StreamMessage::getId).collect(Collectors.toList());
        Assertions.assertEquals(Arrays.<String> asList(id4, id5, id6), recoveredIds, "recoveredIds");
        reader2.close();
    }

    @Test
    void readStreamRecoverManualAckToAutoAck() throws InterruptedException {
        String stream = "stream1";
        String consumerGroup = "readStreamRecoverManualAckToAutoAck";
        Consumer<String> consumer = Consumer.from(consumerGroup, "consumer1");
        final StreamItemReader<String, String> reader = streamReader(stream, consumer);
        reader.setAckPolicy(StreamAckPolicy.MANUAL);
        open(reader);
        String field1 = "field1";
        String value1 = "value1";
        String field2 = "field2";
        String value2 = "value2";
        Map<String, String> body = map(field1, value1, field2, value2);
        sourceConnection.sync().xadd(stream, body);
        sourceConnection.sync().xadd(stream, body);
        sourceConnection.sync().xadd(stream, body);
        List<StreamMessage<String, String>> sourceRecords = new ArrayList<>();
        awaitUntil(() -> sourceRecords.addAll(reader.readMessages()));
        Assertions.assertEquals(3, sourceRecords.size());

        List<StreamMessage<String, String>> recoveredRecords = new ArrayList<>();
        String id4 = sourceConnection.sync().xadd(stream, body);
        String id5 = sourceConnection.sync().xadd(stream, body);
        String id6 = sourceConnection.sync().xadd(stream, body);
        reader.close();

        final StreamItemReader<String, String> reader2 = streamReader(stream, consumer);
        reader2.setAckPolicy(StreamAckPolicy.AUTO);
        open(reader2);

        // Wait until task.poll() doesn't return any more records
        awaitUntil(() -> recoveredRecords.addAll(reader2.readMessages()));
        awaitUntil(() -> !recoveredRecords.addAll(reader2.readMessages()));
        List<String> recoveredIds = recoveredRecords.stream().map(StreamMessage::getId).collect(Collectors.toList());
        Assertions.assertEquals(Arrays.asList(id4, id5, id6), recoveredIds, "recoveredIds");

        PendingMessages pending = sourceConnection.sync().xpending(stream, consumerGroup);
        Assertions.assertEquals(0, pending.getCount(), "pending message count");
        reader2.close();
    }

    @Test
    void readMessages(TestInfo testInfo) throws Exception {
        generateStreams(testInfo, 57);
        List<String> keys = ScanIterator.scan(sourceConnection.sync(), KeyScanArgs.Builder.type(KeyValue.STREAM)).stream()
                .collect(Collectors.toList());
        Consumer<String> consumer = Consumer.from("batchtests-readmessages", "consumer1");
        for (String key : keys) {
            long count = sourceConnection.sync().xlen(key);
            StreamItemReader<String, String> reader = streamReader(key, consumer);
            List<StreamMessage<String, String>> messages = readAll(testInfo, reader);
            assertEquals(count, messages.size());
            assertMessageBody(messages);
            awaitUntil(() -> reader.ack(reader.readMessages()) == 0);
            reader.close();
        }
    }

    @Test
    void streamReaderJob(TestInfo testInfo) throws Exception {
        generateStreams(testInfo, 277);
        List<String> keys = ScanIterator.scan(sourceConnection.sync(), KeyScanArgs.Builder.type(KeyValue.STREAM)).stream()
                .collect(Collectors.toList());
        Consumer<String> consumer = Consumer.from("batchtests-readstreamjob", "consumer1");
        for (String key : keys) {
            long count = sourceConnection.sync().xlen(key);
            List<StreamMessage<String, String>> messages = readAllAndClose(testInfo, streamReader(key, consumer));
            Assertions.assertEquals(count, messages.size());
            assertMessageBody(messages);
        }
    }

    @Test
    void invalidConnection(TestInfo testInfo) throws Exception {
        try (RedisModulesClient badSourceClient = RedisModulesClient.create("redis://badhost:6379")) {
            RedisItemReader<String, String> reader = reader(badSourceClient).struct();
            reader.setName(name(testInfo) + "-reader");
            Assertions.assertThrows(RedisConnectionException.class, () -> open(reader));
        }
    }

    protected static final CompositeItemStreamProcessor<List<Object>, KeyValue<String>, KeyValue<String>> structProcessor = new CompositeItemStreamProcessor<>(
            new KeyValueProcessor<>(StringCodec.UTF8), new StructProcessor<>(StringCodec.UTF8));

    @Test
    void luaHash() throws Exception {
        String key = "myhash";
        Map<String, String> hash = new HashMap<>();
        hash.put("field1", "value1");
        hash.put("field2", "value2");
        sourceConnection.sync().hset(key, hash);
        long ttl = System.currentTimeMillis() + 123456;
        sourceConnection.sync().pexpireat(key, ttl);
        KeyValueReadOperation<String, String> operation = KeyValueReadOperation.builder(sourceClient).struct();
        KeyValue<String> ds = structProcessor.process(operation.execute(sourceConnection.async(), key).get());
        Assertions.assertEquals(key, ds.getKey());
        Assertions.assertEquals(ttl, ds.getTtl());
        Assertions.assertEquals(KeyValue.HASH, ds.getType());
        Assertions.assertEquals(hash, ds.getValue());
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Test
    void luaZset() throws Exception {
        String key = "myzset";
        ScoredValue[] values = { ScoredValue.just(123.456, "value1"), ScoredValue.just(654.321, "value2") };
        sourceConnection.sync().zadd(key, values);
        KeyValueReadOperation<String, String> operation = KeyValueReadOperation.builder(sourceClient).struct();
        KeyValue<String> ds = structProcessor.process(operation.execute(sourceConnection.async(), key).get());
        Assertions.assertEquals(key, ds.getKey());
        Assertions.assertEquals(KeyValue.ZSET, ds.getType());
        Assertions.assertEquals(new HashSet<>(Arrays.asList(values)), ds.getValue());
    }

    @Test
    void luaList() throws Exception {
        String key = "mylist";
        List<String> values = Arrays.asList("value1", "value2");
        sourceConnection.sync().rpush(key, values.toArray(new String[0]));
        KeyValueReadOperation<String, String> operation = KeyValueReadOperation.builder(sourceClient).struct();
        KeyValue<String> ds = structProcessor.process(operation.execute(sourceConnection.async(), key).get());
        Assertions.assertEquals(key, ds.getKey());
        Assertions.assertEquals(KeyValue.LIST, ds.getType());
        Assertions.assertEquals(values, ds.getValue());
    }

    @Test
    void luaStream() throws Exception {
        String key = "mystream";
        Map<String, String> body = new HashMap<>();
        body.put("field1", "value1");
        body.put("field2", "value2");
        sourceConnection.sync().xadd(key, body);
        sourceConnection.sync().xadd(key, body);
        KeyValueReadOperation<String, String> operation = KeyValueReadOperation.builder(sourceClient).struct();
        KeyValue<String> ds = structProcessor.process(operation.execute(sourceConnection.async(), key).get());
        Assertions.assertEquals(key, ds.getKey());
        Assertions.assertEquals(KeyValue.STREAM, ds.getType());
        List<StreamMessage<String, String>> messages = ds.getValue();
        Assertions.assertEquals(2, messages.size());
        for (StreamMessage<String, String> message : messages) {
            Assertions.assertEquals(body, message.getBody());
            Assertions.assertNotNull(message.getId());
        }
    }

    protected static final KeyValueProcessor<byte[], byte[]> dumpProcessor = new KeyValueProcessor<>(ByteArrayCodec.INSTANCE);

    @Test
    void luaStreamDump() throws Exception {
        String key = "mystream";
        Map<String, String> body = new HashMap<>();
        body.put("field1", "value1");
        body.put("field2", "value2");
        sourceConnection.sync().xadd(key, body);
        sourceConnection.sync().xadd(key, body);
        long ttl = System.currentTimeMillis() + 123456;
        sourceConnection.sync().pexpireat(key, ttl);
        StatefulRedisModulesConnection<byte[], byte[]> byteConnection = RedisModulesUtils.connection(sourceClient,
                ByteArrayCodec.INSTANCE);
        KeyValueReadOperation<byte[], byte[]> operation = KeyValueReadOperation.builder(sourceClient, ByteArrayCodec.INSTANCE)
                .dump();
        KeyValue<byte[]> dump = dumpProcessor.process(operation.execute(byteConnection.async(), toByteArray(key)).get());
        Assertions.assertArrayEquals(toByteArray(key), dump.getKey());
        Assertions.assertTrue(Math.abs(ttl - dump.getTtl()) <= 3);
        sourceConnection.sync().del(key);
        sourceConnection.sync().restore(key, dump.getValue(), RestoreArgs.Builder.ttl(ttl).absttl());
        Assertions.assertEquals(KeyValue.STREAM, sourceConnection.sync().type(key));
    }

    private byte[] toByteArray(String key) {
        return Utils.toByteArrayKeyFunction(StringCodec.UTF8).apply(key);
    }

    private String toString(byte[] key) {
        return Utils.toStringKeyFunction(ByteArrayCodec.INSTANCE).apply(key);
    }

    protected void open(ItemStream itemStream) {
        itemStream.open(new ExecutionContext());
    }

    protected static final CompositeItemStreamProcessor<List<Object>, KeyValue<byte[]>, KeyValue<byte[]>> bytesStructProcessor = new CompositeItemStreamProcessor<>(
            new KeyValueProcessor<>(ByteArrayCodec.INSTANCE), new StructProcessor<>(ByteArrayCodec.INSTANCE));

    @Test
    void luaStreamByteArray() throws Exception {
        String key = "mystream";
        Map<String, String> body = new HashMap<>();
        body.put("field1", "value1");
        body.put("field2", "value2");
        sourceConnection.sync().xadd(key, body);
        sourceConnection.sync().xadd(key, body);
        KeyValueReadOperation<byte[], byte[]> operation = KeyValueReadOperation.builder(sourceClient, ByteArrayCodec.INSTANCE)
                .struct();
        StatefulRedisModulesConnection<byte[], byte[]> byteConnection = RedisModulesUtils.connection(sourceClient,
                ByteArrayCodec.INSTANCE);
        KeyValue<byte[]> ds = bytesStructProcessor.process(operation.execute(byteConnection.async(), toByteArray(key)).get());
        Assertions.assertArrayEquals(toByteArray(key), ds.getKey());
        Assertions.assertEquals(KeyValue.STREAM, ds.getType());
        List<StreamMessage<byte[], byte[]>> messages = ds.getValue();
        Assertions.assertEquals(2, messages.size());
        for (StreamMessage<byte[], byte[]> message : messages) {
            Map<byte[], byte[]> actual = message.getBody();
            Assertions.assertEquals(2, actual.size());
            Map<String, String> actualString = new HashMap<>();
            actual.forEach((k, v) -> actualString.put(toString(k), toString(v)));
            Assertions.assertEquals(body, actualString);
        }
    }

    @Test
    void luaHLL() throws Exception {
        String key1 = "hll:1";
        sourceConnection.sync().pfadd(key1, "member:1", "member:2");
        String key2 = "hll:2";
        sourceConnection.sync().pfadd(key2, "member:1", "member:2", "member:3");
        KeyValueReadOperation<String, String> operation = KeyValueReadOperation.builder(sourceClient).struct();
        KeyValue<String> ds1 = structProcessor.process(operation.execute(sourceConnection.async(), key1).get());
        Assertions.assertEquals(key1, ds1.getKey());
        Assertions.assertEquals(KeyValue.STRING, ds1.getType());
        Assertions.assertEquals(sourceConnection.sync().get(key1), ds1.getValue());
    }

}
