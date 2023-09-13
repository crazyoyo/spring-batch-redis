package com.redis.spring.batch.test;

import java.time.Duration;
import java.util.List;
import java.util.stream.Collectors;

import org.awaitility.Awaitility;
import org.awaitility.core.ConditionTimeoutException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.support.SimpleFlow;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.core.task.SimpleAsyncTaskExecutor;

import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.api.sync.RedisModulesCommands;
import com.redis.lettucemod.util.RedisModulesUtils;
import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.RedisItemWriter;
import com.redis.spring.batch.gen.DataType;
import com.redis.spring.batch.gen.GeneratorItemReader;
import com.redis.spring.batch.util.BatchUtils;
import com.redis.spring.batch.util.KeyComparison;
import com.redis.spring.batch.util.KeyComparison.Status;
import com.redis.spring.batch.util.KeyComparisonItemReader;
import com.redis.spring.batch.util.Range;
import com.redis.testcontainers.RedisServer;

import io.lettuce.core.AbstractRedisClient;

public abstract class AbstractTargetTestBase extends AbstractTestBase {

    public static final Duration COMPARE_TIMEOUT = Duration.ofSeconds(3);

    protected AbstractRedisClient targetClient;

    protected StatefulRedisModulesConnection<String, String> targetConnection;

    protected RedisModulesCommands<String, String> targetCommands;

    protected abstract RedisServer getTargetRedisServer();

    @BeforeAll
    void setupTarget() {
        getTargetRedisServer().start();
        targetClient = client(getTargetRedisServer());
        targetConnection = RedisModulesUtils.connection(targetClient);
        targetCommands = targetConnection.sync();
    }

    @AfterAll
    void teardownTarget() {
        targetConnection.close();
        targetClient.shutdown();
        targetClient.getResources().shutdown();
        getTargetRedisServer().close();
    }

    @BeforeEach
    void flushAllTarget() {
        targetCommands.flushall();
        awaitEquals(() -> 0L, targetCommands::dbsize);
    }

    protected void awaitCompare(TestInfo info) {
        Awaitility.await().timeout(COMPARE_TIMEOUT).until(() -> compare(info));
    }

    /**
     * 
     * @param left
     * @param right
     * @return
     * @return list of differences
     * @throws Exception
     */
    protected boolean compare(TestInfo info) throws Exception {
        if (commands.dbsize().equals(0L)) {
            log.info("Source database is empty");
            return false;
        }
        if (!commands.dbsize().equals(targetCommands.dbsize())) {
            log.info("Source and target databases have different sizes");
            return false;
        }
        KeyComparisonItemReader reader = comparisonReader(testInfo(info, "compare", "reader"));
        reader.open(new ExecutionContext());
        List<KeyComparison> comparisons = BatchUtils.readAll(reader);
        reader.close();
        Assertions.assertFalse(comparisons.isEmpty());
        List<KeyComparison> diffs = comparisons.stream().filter(c -> c.getStatus() != Status.OK).collect(Collectors.toList());
        diffs.forEach(this::logComparison);
        return diffs.isEmpty();
    }

    protected KeyComparisonItemReader comparisonReader(TestInfo info) throws Exception {
        KeyComparisonItemReader reader = new KeyComparisonItemReader(structReader(info, client),
                structReader(info, targetClient).keyValueProcessor());
        reader.setName(name(info));
        reader.setTtlTolerance(Duration.ofMillis(100));
        return reader;
    }

    protected <K, V> boolean liveReplication(TestInfo testInfo, RedisItemReader<K, V> reader, RedisItemWriter<K, V> writer,
            RedisItemReader<K, V> liveReader, RedisItemWriter<K, V> liveWriter) throws Exception {
        GeneratorItemReader gen = new GeneratorItemReader();
        gen.setMaxItemCount(300);
        gen.setTypes(DataType.HASH, DataType.LIST, DataType.SET, DataType.STREAM, DataType.STRING, DataType.ZSET);
        generate(testInfo(testInfo, "generate"), gen);
        TaskletStep step = step(testInfo(testInfo, "step"), reader, writer).build();
        SimpleFlow flow = new FlowBuilder<SimpleFlow>(name(testInfo(testInfo, "snapshotFlow"))).start(step).build();
        setLive(liveReader);
        TaskletStep liveStep = flushingStep(testInfo(testInfo, "liveStep"), liveReader, liveWriter).build();
        SimpleFlow liveFlow = new FlowBuilder<SimpleFlow>(name(testInfo(testInfo, "liveFlow"))).start(liveStep).build();
        Job job = job(testInfo).start(new FlowBuilder<SimpleFlow>(name(testInfo(testInfo, "flow")))
                .split(new SimpleAsyncTaskExecutor()).add(liveFlow, flow).build()).build().build();
        JobExecution execution = runAsync(job);
        GeneratorItemReader liveGen = new GeneratorItemReader();
        liveGen.setMaxItemCount(700);
        liveGen.setTypes(DataType.HASH, DataType.LIST, DataType.SET, DataType.STRING, DataType.ZSET);
        liveGen.setExpiration(Range.of(100));
        liveGen.setKeyRange(Range.from(300));
        generate(testInfo(testInfo, "generateLive"), liveGen);
        try {
            awaitTermination(execution);
        } catch (ConditionTimeoutException e) {
            // ignore
        }
        awaitClosed(reader);
        awaitClosed(writer);
        awaitClosed(liveReader);
        awaitClosed(liveWriter);
        return compare(testInfo);
    }

    protected void logComparison(KeyComparison comparison) {
        log.error(comparison.toString());
        if (comparison.getStatus() == Status.VALUE) {
            log.error("Expected {} but was {}", comparison.getSource().getValue(), comparison.getTarget().getValue());
        }
    }

}
