package org.springframework.batch.item.redis;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.junit.Assert;
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
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.DefaultBufferedReaderFactory;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.separator.DefaultRecordSeparatorPolicy;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.redis.support.KeyValueItemComparator;
import org.springframework.batch.item.redis.support.ReaderOptions;
import org.springframework.batch.item.redis.support.RedisDataStructureItemWriters;
import org.springframework.batch.item.redis.support.TypeKeyValue;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.batch.item.support.ListItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.Map;


@SpringBootTest(classes = BatchTestApplication.class)
@RunWith(SpringRunner.class)
public class SpringBatchIntegrationTest extends BaseTest {

    private final static Logger log = LoggerFactory.getLogger(SpringBatchIntegrationTest.class);

    @Autowired
    private JobLauncher jobLauncher;
    @Autowired
    private JobLauncher asyncJobLauncher;
    @Autowired
    private JobBuilderFactory jobBuilderFactory;
    @Autowired
    private StepBuilderFactory stepBuilderFactory;
    @Autowired
    private RedisClient redisClient;
    @Autowired
    private RedisClient targetRedisClient;
    @Autowired
    private StatefulRedisConnection<String, String> connection;
    @Autowired
    private StatefulRedisConnection<String, String> targetConnection;
    @Autowired
    private GenericObjectPool<StatefulRedisConnection<String, String>> pool;
    @Autowired
    private GenericObjectPool<StatefulRedisConnection<String, String>> targetPool;

    private void redisWriter(String name) throws IOException, JobParametersInvalidException, JobExecutionAlreadyRunningException, JobRestartException, JobInstanceAlreadyCompleteException {
        FlatFileItemReader<Map<String, String>> reader = fileReader(new ClassPathResource("beers.csv"));
        RedisDataStructureItemWriters.RedisHashItemWriter<String, String, Map<String, String>> writer = new RedisDataStructureItemWriters.RedisHashItemWriter<>(pool, RedisURI.DEFAULT_TIMEOUT, m -> m.get(Beers.FIELD_ID), m -> m);
        TaskletStep step = stepBuilderFactory.get(name + "-step").<Map<String, String>, Map<String, String>>chunk(10).reader(reader).writer(writer).build();
        Job job = jobBuilderFactory.get(name + "-job").start(step).build();
        jobLauncher.run(job, new JobParameters());
    }

    @Test
    public void testRedisWriter() throws Exception {
        redisWriter("redis-writer");
        assertSize(connection);
    }

    private void assertSize(StatefulRedisConnection<String, String> connection) {
        Assert.assertEquals(Beers.SIZE, (long) connection.sync().dbsize());
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
        RedisKeyValueItemReader<String, String> reader = RedisKeyValueItemReader.<String, String>builder().pool(pool).keyReader(RedisKeyItemReader.builder().redisClient(redisClient).build()).options(ReaderOptions.builder().build()).build();
        ListItemWriter<TypeKeyValue<String>> writer = new ListItemWriter<>();
        JobExecution execution = execute("scan-reader", reader, writer);
        Assert.assertTrue(execution.getAllFailureExceptions().isEmpty());
        Assert.assertEquals(Beers.SIZE, writer.getWrittenItems().size());
    }

    @Test
    public void testReplication() throws Exception {
        DataPopulator.builder().redisClient(redisClient).start(0).end(1039).build().run();
        RedisKeyDumpItemReader<String, String> reader = RedisKeyDumpItemReader.<String, String>builder().keyReader(RedisKeyItemReader.builder().redisClient(redisClient).build()).pool(pool).options(ReaderOptions.builder().build()).build();
        RedisKeyDumpItemWriter<String, String> writer = RedisKeyDumpItemWriter.<String, String>builder().commandTimeout(RedisURI.DEFAULT_TIMEOUT).pool(targetPool).replace(true).build();
        execute("replication", reader, writer);
        compare("replication-comparison");
    }

    private <I, O> JobExecution execute(String name, ItemReader<? extends I> reader, ItemWriter<? super O> writer) throws JobParametersInvalidException, JobExecutionAlreadyRunningException, JobRestartException, JobInstanceAlreadyCompleteException {
        Job job = job(name, 50, reader, writer);
        return jobLauncher.run(job, new JobParameters());
    }

    private <I, O> Job job(String name, int batchSize, ItemReader<? extends I> reader, ItemWriter<? super O> writer) {
        TaskletStep step = stepBuilderFactory.get(name + "-step").<I, O>chunk(batchSize).reader(reader).writer(writer).build();
        return jobBuilderFactory.get(name + "-job").start(step).build();
    }

    @Test
    public void testLiveReplication() throws Exception {
        DataPopulator.builder().redisClient(redisClient).start(0).end(1000).build().run();
        RedisKeyDumpItemReader<String, String> reader = RedisKeyDumpItemReader.<String, String>builder().keyReader(RedisLiveKeyItemReader.builder().redisClient(redisClient).build()).pool(pool).options(ReaderOptions.builder().build()).build();
        RedisKeyDumpItemWriter<String, String> writer = RedisKeyDumpItemWriter.<String, String>builder().commandTimeout(RedisURI.DEFAULT_TIMEOUT).pool(targetPool).replace(true).build();
        Job job = job("live-replication", 50, reader, writer);
        JobExecution execution = asyncJobLauncher.run(job, new JobParameters());
        DataPopulator.builder().redisClient(redisClient).start(1000).end(2000).sleep(1L).build().run();
        Thread.sleep(100);
        reader.flush();
        Thread.sleep(100);
        ((RedisLiveKeyItemReader<String, String>) reader.getKeyReader()).stop();
        while (execution.isRunning()) {
            Thread.sleep(100);
        }
        compare("live-replication-comparison");
    }

    private void compare(String name) throws JobParametersInvalidException, JobExecutionAlreadyRunningException, JobRestartException, JobInstanceAlreadyCompleteException {
        RedisCommands<String, String> sourceCommands = connection.sync();
        RedisCommands<String, String> targetCommands = targetConnection.sync();
        Assert.assertEquals(sourceCommands.dbsize(), targetCommands.dbsize());
        RedisKeyValueItemReader<String, String> reader = RedisKeyValueItemReader.<String, String>builder().pool(pool).keyReader(RedisKeyItemReader.builder().redisClient(redisClient).build()).options(ReaderOptions.builder().build()).build();
        KeyValueItemComparator<String, String> comparator = KeyValueItemComparator.<String, String>builder().targetReader(RedisKeyValueItemReader.<String, String>builder().pool(targetPool).keyReader(RedisKeyItemReader.builder().redisClient(targetRedisClient).build()).options(ReaderOptions.builder().build()).build()).ttlTolerance(1).build();
        execute(name, reader, comparator);
        Assert.assertEquals(Math.toIntExact(sourceCommands.dbsize()), comparator.getOk().size());
    }

    @Test
    public void testStringItemWriter() throws Exception {
        RedisDataStructureItemWriters.RedisStringItemWriter<String, String, Map<String, String>> writer = new RedisDataStructureItemWriters.RedisStringItemWriter<>(pool, RedisURI.DEFAULT_TIMEOUT, m -> m.get(Beers.FIELD_ID), m -> m.get(Beers.FIELD_NAME));
        run("string-item-writer", beerReader(), writer);
        assertSize(connection);
        Assert.assertEquals("Redband Stout", connection.sync().get("371"));
    }

    @Test
    public void testSetItemWriter() throws Exception {
        RedisDataStructureItemWriters.RedisSetItemWriter<String, String, Map<String, String>> writer = new RedisDataStructureItemWriters.RedisSetItemWriter<>(pool, RedisURI.DEFAULT_TIMEOUT, m -> "beers", m -> m.get(Beers.FIELD_ID));
        run("set-item-writer", beerReader(), writer);
        Assert.assertEquals(Beers.SIZE, (long) connection.sync().scard("beers"));
    }

    @Test
    public void testStreamItemWriter() throws Exception {
        RedisDataStructureItemWriters.RedisStreamItemWriter<String, String, Map<String, String>> writer = new RedisDataStructureItemWriters.RedisStreamItemWriter<>(pool, RedisURI.DEFAULT_TIMEOUT, m -> "beers", m -> m);
        run("stream-item-writer", beerReader(), writer);
        Assert.assertEquals(Beers.SIZE, (long) connection.sync().xlen("beers"));
    }

    private ItemReader<Map<String, String>> beerReader() throws IOException {
        return new ListItemReader<>(Beers.load());
    }

    private <T> void run(String name, ItemReader<T> reader, ItemWriter<T> writer) throws Exception {
        TaskletStep step = stepBuilderFactory.get(name + "-step").<T, T>chunk(50).reader(reader).writer(writer).build();
        Job job = jobBuilderFactory.get(name + "-job").start(step).build();
        jobLauncher.run(job, new JobParameters());
    }

}