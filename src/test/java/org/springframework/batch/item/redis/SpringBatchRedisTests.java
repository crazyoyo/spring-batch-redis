package org.springframework.batch.item.redis;

import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.sync.RedisCommands;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.junit.Assert;
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
import org.springframework.batch.item.redis.support.*;
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
import java.util.function.Function;


@SpringBootTest(classes = BatchTestApplication.class)
@RunWith(SpringRunner.class)
public class SpringBatchRedisTests extends BaseTest {


    @Autowired
    private JobLauncher jobLauncher;
    @Autowired
    private JobLauncher asyncJobLauncher;
    @Autowired
    private JobBuilderFactory jobBuilderFactory;
    @Autowired
    private StepBuilderFactory stepBuilderFactory;
    @Autowired
    private RedisURI redisURI;
    @Autowired
    private RedisURI targetRedisURI;
    @Autowired
    private StatefulRedisConnection<String, String> connection;
    @Autowired
    private StatefulRedisConnection<String, String> targetConnection;
    @Autowired
    private GenericObjectPool<StatefulRedisConnection<String, String>> pool;
    @Autowired
    private GenericObjectPool<StatefulRedisConnection<String, String>> targetPool;

    private void redisWriter(String name) throws Exception {
        FlatFileItemReader<Map<String, String>> reader = fileReader(new ClassPathResource("beers.csv"));
        CommandItemWriters.Hmset<String, String, Map<String, String>> writer = new CommandItemWriters.Hmset<>(pool, async(), RedisURI.DEFAULT_TIMEOUT_DURATION, m -> m.get(Beers.FIELD_ID), m -> m);
        run(name, reader, writer);
    }

    private Function<StatefulConnection<String, String>, BaseRedisAsyncCommands<String, String>> async() {
        return c -> ((StatefulRedisConnection<String, String>) c).async();
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
        RedisKeyValueItemReader<String> reader = RedisKeyValueItemReader.builder().redisURI(redisURI).build();
        ListItemWriter<KeyValue<String>> writer = new ListItemWriter<>();
        JobExecution execution = run("scan-reader", reader, writer);
        Assert.assertTrue(execution.getAllFailureExceptions().isEmpty());
        Assert.assertEquals(Beers.SIZE, writer.getWrittenItems().size());
    }

    @Test
    public void testReplication() throws Exception {
        DataPopulator.builder().connection(connection).start(0).end(1039).build().run();
        RedisKeyDumpItemReader<String> reader = RedisKeyDumpItemReader.builder().redisURI(redisURI).build();
        RedisKeyDumpItemWriter<String, String> writer = RedisKeyDumpItemWriter.builder().redisURI(targetRedisURI).replace(true).build();
        run("replication", reader, writer);
        compare("replication-comparison");
    }

    @Test
    public void testLiveReplication() throws Exception {
        DataPopulator.builder().connection(connection).start(0).end(1000).build().run();
        RedisKeyDumpItemReader<String> reader = RedisKeyDumpItemReader.builder().redisURI(redisURI).live(true).threads(2).build();
        LiveKeyItemReader<String, String> keyReader = (LiveKeyItemReader<String, String>) reader.getKeyReader();
        RedisKeyDumpItemWriter<String, String> writer = RedisKeyDumpItemWriter.builder().redisURI(targetRedisURI).replace(true).build();
        Job job = job("live-replication", reader, writer);
        JobExecution execution = asyncJobLauncher.run(job, new JobParameters());
        while (!keyReader.isRunning()) {
            Thread.sleep(1);
        }
        DataPopulator.builder().connection(connection).start(1000).end(2000).sleep(1L).build().run();
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
        RedisCommands<String, String> sourceCommands = connection.sync();
        RedisCommands<String, String> targetCommands = targetConnection.sync();
        Assert.assertEquals(sourceCommands.dbsize(), targetCommands.dbsize());
        RedisKeyValueItemReader<String> reader = RedisKeyValueItemReader.builder().redisURI(redisURI).build();
        KeyValueItemComparator<String> comparator = new KeyValueItemComparator<>(reader.getKeyValueProcessor(), 1);
        run(name, reader, comparator);
        Assert.assertEquals(Math.toIntExact(sourceCommands.dbsize()), comparator.getOk().size());
    }

    @Test
    public void testStringItemWriter() throws Exception {
        run("string-item-writer", beerReader(), CommandItemWriters.Set.<Map<String, String>>builder().redisURI(redisURI).keyConverter(m -> m.get(Beers.FIELD_ID)).valueConverter(m -> m.get(Beers.FIELD_NAME)).build());
        assertSize(connection);
        Assert.assertEquals("Redband Stout", connection.sync().get("371"));
    }

    @Test
    public void testSetItemWriter() throws Exception {
        run("set-item-writer", beerReader(), CommandItemWriters.Sadd.<Map<String,String>>builder().redisURI(redisURI).keyConverter(m -> "beers").memberIdConverter(m -> m.get(Beers.FIELD_ID)).build());
        Assert.assertEquals(Beers.SIZE, (long) connection.sync().scard("beers"));
    }

    @Test
    public void testStreamItemWriter() throws Exception {
        CommandItemWriters.Xadd<String, String, Map<String, String>> writer = new CommandItemWriters.Xadd<>(pool, async(), RedisURI.DEFAULT_TIMEOUT_DURATION, m -> "beers", m -> m, null, null, false);
        run("stream-item-writer", beerReader(), writer);
        Assert.assertEquals(Beers.SIZE, (long) connection.sync().xlen("beers"));
    }

    private ItemReader<Map<String, String>> beerReader() throws IOException {
        return new ListItemReader<>(Beers.load());
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
        RedisKeyDumpItemReader<String> reader = RedisKeyDumpItemReader.builder().redisURI(sourceURI).live(true).threads(2).build();
        RedisURI targetURI = RedisURI.create("rediss://target:6379");
        RedisKeyDumpItemWriter<String, String> writer = RedisKeyDumpItemWriter.builder().redisURI(targetURI).replace(true).build();
        TaskletStep step = stepBuilderFactory.get("step").<KeyDump<String>, KeyDump<String>>chunk(50).reader(reader).writer(writer).build();
        jobBuilderFactory.get("job").start(step).build();
    }


}