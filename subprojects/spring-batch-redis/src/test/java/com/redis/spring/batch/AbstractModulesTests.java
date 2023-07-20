package com.redis.spring.batch;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.function.Function;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.support.IteratorItemReader;
import org.springframework.batch.item.support.ListItemReader;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.redis.lettucemod.Beers;
import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.api.sync.RedisModulesCommands;
import com.redis.lettucemod.search.IndexInfo;
import com.redis.lettucemod.search.Suggestion;
import com.redis.lettucemod.timeseries.AddOptions;
import com.redis.lettucemod.timeseries.DuplicatePolicy;
import com.redis.lettucemod.timeseries.RangeOptions;
import com.redis.lettucemod.timeseries.Sample;
import com.redis.lettucemod.timeseries.TimeRange;
import com.redis.lettucemod.util.RedisModulesUtils;
import com.redis.spring.batch.common.KeyValue;
import com.redis.spring.batch.common.Utils;
import com.redis.spring.batch.convert.SampleConverter;
import com.redis.spring.batch.convert.SuggestionConverter;
import com.redis.spring.batch.reader.GeneratorItemReader;
import com.redis.spring.batch.reader.GeneratorItemReader.Type;
import com.redis.spring.batch.reader.KeyComparison.Status;
import com.redis.spring.batch.reader.KeyComparisonItemReader;
import com.redis.spring.batch.reader.KeyEvent;
import com.redis.spring.batch.reader.KeyValueReadOperation;
import com.redis.spring.batch.reader.KeyspaceNotification;
import com.redis.spring.batch.reader.KeyspaceNotificationItemReader;
import com.redis.spring.batch.writer.KeyComparisonCountItemWriter;
import com.redis.spring.batch.writer.OperationItemWriter;
import com.redis.spring.batch.writer.operation.JsonDel;
import com.redis.spring.batch.writer.operation.JsonSet;
import com.redis.spring.batch.writer.operation.Sugadd;
import com.redis.spring.batch.writer.operation.SugaddIncr;
import com.redis.spring.batch.writer.operation.TsAdd;

import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.StringCodec;

abstract class AbstractModulesTests extends AbstractTargetTests {

    private static final String JSON_BEER_1 = "{\"id\":\"1\",\"brewery_id\":\"812\",\"name\":\"Hocus Pocus\",\"abv\":\"4.5\",\"ibu\":\"0\",\"srm\":\"0\",\"upc\":\"0\",\"filepath\":\"\",\"descript\":\"Our take on a classic summer ale.  A toast to weeds, rays, and summer haze.  A light, crisp ale for mowing lawns, hitting lazy fly balls, and communing with nature, Hocus Pocus is offered up as a summer sacrifice to clodless days.\\n\\nIts malty sweetness finishes tart and crisp and is best apprediated with a wedge of orange.\",\"add_user\":\"0\",\"last_mod\":\"2010-07-22 20:00:20 UTC\",\"style_name\":\"Light American Wheat Ale or Lager\",\"cat_name\":\"Other Style\"}";

    private static final int BEER_COUNT = 1019;

    @Test
    void readKeyspaceNotifications(TestInfo testInfo) throws Exception {
        enableKeyspaceNotifications(sourceClient);
        KeyspaceNotificationItemReader<String, String> reader = new KeyspaceNotificationItemReader<>(sourceClient,
                StringCodec.UTF8);
        reader.open(new ExecutionContext());
        GeneratorItemReader gen = new GeneratorItemReader();
        gen.setMaxItemCount(100);
        gen.setTypes(
                Arrays.asList(Type.HASH, Type.LIST, Type.SET, Type.STREAM, Type.STRING, Type.ZSET, Type.TIMESERIES, Type.JSON));
        generate(testInfo, gen);
        awaitUntil(() -> reader.getQueue().size() > 0);
        Assertions.assertEquals(KeyEvent.SET, reader.getQueue().remove().getEvent());
        Set<KeyEvent> eventTypes = new LinkedHashSet<>(Arrays.asList(KeyEvent.SET, KeyEvent.HSET, KeyEvent.JSON_SET,
                KeyEvent.RPUSH, KeyEvent.SADD, KeyEvent.ZADD, KeyEvent.XADD, KeyEvent.TS_ADD));
        KeyspaceNotification notification;
        while ((notification = reader.getQueue().poll()) != null) {
            Assertions.assertTrue(eventTypes.contains(notification.getEvent()));
        }
        reader.close();
    }

    @Test
    void readKeyspaceNotificationsWithType(TestInfo testInfo) throws Exception {
        enableKeyspaceNotifications(sourceClient);
        KeyspaceNotificationItemReader<String, String> reader = new KeyspaceNotificationItemReader<>(sourceClient,
                StringCodec.UTF8);
        reader.getScanOptions().setType(KeyValue.HASH);
        reader.open(new ExecutionContext());
        GeneratorItemReader gen = new GeneratorItemReader();
        gen.setMaxItemCount(100);
        gen.setTypes(
                Arrays.asList(Type.HASH, Type.LIST, Type.SET, Type.STREAM, Type.STRING, Type.ZSET, Type.TIMESERIES, Type.JSON));
        generate(testInfo, gen);
        BlockingQueue<KeyspaceNotification> queue = reader.getQueue();
        awaitUntil(() -> reader.getQueue().size() > 0);
        KeyspaceNotification notification;
        while ((notification = queue.poll()) != null) {
            Assertions.assertEquals(KeyValue.HASH, notification.getEvent().getType());
        }
        reader.close();
    }

    @Test
    void writeJsonSet(TestInfo testInfo) throws Exception {
        JsonSet<String, String, JsonNode> jsonSet = new JsonSet<>(n -> "beer:" + n.get("id").asText(), JsonNode::toString,
                t -> ".");
        OperationItemWriter<String, String, JsonNode> writer = new OperationItemWriter<>(sourceClient, StringCodec.UTF8,
                jsonSet);
        IteratorItemReader<JsonNode> reader = new IteratorItemReader<>(Beers.jsonNodeIterator());
        run(testInfo, reader, writer);
        Assertions.assertEquals(BEER_COUNT, sourceConnection.sync().keys("beer:*").size());
        Assertions.assertEquals(new ObjectMapper().readTree(JSON_BEER_1),
                new ObjectMapper().readTree(sourceConnection.sync().jsonGet("beer:1")));
    }

    @Test
    void writeJsonDel(TestInfo testInfo) throws Exception {
        GeneratorItemReader gen = new GeneratorItemReader();
        gen.setTypes(Type.JSON);
        gen.setMaxItemCount(DEFAULT_GENERATOR_COUNT);
        generate(testInfo, gen);
        JsonDel<String, String, KeyValue<String>> jsonDel = new JsonDel<>(KeyValue::getKey);
        run(testInfo, gen, new OperationItemWriter<>(sourceClient, StringCodec.UTF8, jsonDel));
        Assertions.assertEquals(0, sourceConnection.sync().dbsize());
    }

    @Test
    void writeTs(TestInfo testInfo) throws Exception {
        String key = "ts:1";
        Random random = new Random();
        int count = 100;
        List<Sample> samples = new ArrayList<>(count);
        for (int index = 0; index < count; index++) {
            long timestamp = System.currentTimeMillis() - count + (index % (count / 2));
            samples.add(Sample.of(timestamp, random.nextDouble()));
        }
        ListItemReader<Sample> reader = new ListItemReader<>(samples);
        AddOptions<String, String> addOptions = AddOptions.<String, String> builder().policy(DuplicatePolicy.LAST).build();
        TsAdd<String, String, Sample> tsadd = new TsAdd<>(t -> key, Function.identity(), t -> addOptions);
        OperationItemWriter<String, String, Sample> writer = new OperationItemWriter<>(sourceClient, StringCodec.UTF8, tsadd);
        run(testInfo, reader, writer);
        Assertions.assertEquals(count / 2,
                sourceConnection.sync().tsRange(key, TimeRange.unbounded(), RangeOptions.builder().build()).size(), 2);
    }

    @Test
    void beerIndex() throws Exception {
        Beers.populateIndex(sourceConnection);
        IndexInfo indexInfo = RedisModulesUtils.indexInfo(sourceConnection.sync().ftInfo(Beers.INDEX));
        Assertions.assertEquals(BEER_COUNT, indexInfo.getNumDocs());
    }

    @Test
    void tsComparator(TestInfo testInfo) throws Exception {
        sourceConnection.sync().tsAdd("ts:1", Sample.of(123));
        KeyComparisonItemReader reader = comparisonReader();
        KeyComparisonCountItemWriter writer = new KeyComparisonCountItemWriter();
        run(testInfo, reader, writer);
        Assertions.assertEquals(1, writer.getResults().getCount(Status.MISSING));
    }

    @Test
    void testLuaTimeSeries() throws Exception {
        String key = "myts";
        Sample[] samples = { Sample.of(System.currentTimeMillis(), 1.1), Sample.of(System.currentTimeMillis() + 10, 2.2) };
        for (Sample sample : samples) {
            sourceConnection.sync().tsAdd(key, sample);
        }
        KeyValueReadOperation<String, String> operation = KeyValueReadOperation.builder(sourceClient).struct();
        KeyValue<String> ds = structProcessor.process(operation.execute(sourceConnection.async(), key).get());
        Assertions.assertEquals(key, ds.getKey());
        Assertions.assertEquals(KeyValue.TIMESERIES, ds.getType());
        Assertions.assertEquals(Arrays.asList(samples), ds.getValue());
    }

    @Test
    void testLuaTimeSeriesByteArray() throws Exception {
        String key = "myts";
        Sample[] samples = { Sample.of(System.currentTimeMillis(), 1.1), Sample.of(System.currentTimeMillis() + 10, 2.2) };
        for (Sample sample : samples) {
            sourceConnection.sync().tsAdd(key, sample);
        }
        KeyValueReadOperation<byte[], byte[]> operation = KeyValueReadOperation.builder(sourceClient, ByteArrayCodec.INSTANCE)
                .struct();
        StatefulRedisModulesConnection<byte[], byte[]> byteConnection = RedisModulesUtils.connection(sourceClient,
                ByteArrayCodec.INSTANCE);
        Function<String, byte[]> toByteArrayKeyFunction = Utils.toByteArrayKeyFunction(StringCodec.UTF8);
        KeyValue<byte[]> ds = bytesStructProcessor
                .process(operation.execute(byteConnection.async(), toByteArrayKeyFunction.apply(key)).get());
        Assertions.assertArrayEquals(toByteArrayKeyFunction.apply(key), ds.getKey());
        Assertions.assertEquals(KeyValue.TIMESERIES, ds.getType());
        Assertions.assertEquals(Arrays.asList(samples), ds.getValue());
    }

    @Test
    void writeSamples(TestInfo testInfo) throws Exception {
        String key = "ts:1";
        List<Sample> values = new ArrayList<>();
        for (int index = 0; index < 100; index++) {
            values.add(Sample.of(System.currentTimeMillis() - 1000 + index, index));
        }
        ListItemReader<Sample> reader = new ListItemReader<>(values);
        SampleConverter<Sample> converter = new SampleConverter<>(Sample::getTimestamp, Sample::getValue);
        TsAdd<String, String, Sample> tsAdd = new TsAdd<>(t -> key, converter);
        OperationItemWriter<String, String, Sample> writer = new OperationItemWriter<>(sourceClient, StringCodec.UTF8, tsAdd);
        run(testInfo, reader, writer);
        RedisModulesCommands<String, String> sync = sourceConnection.sync();
        assertEquals(1, sync.dbsize());
        assertEquals(values.size(), sync.tsRange(key, TimeRange.unbounded()).size());
    }

    @Test
    void writeSug(TestInfo testInfo) throws Exception {
        String key = "sugadd";
        List<Suggestion<String>> values = new ArrayList<>();
        for (int index = 0; index < 100; index++) {
            values.add(Suggestion.string("word" + index).score(index + 1).payload("payload" + index).build());
        }
        ListItemReader<Suggestion<String>> reader = new ListItemReader<>(values);
        SuggestionConverter<String, Suggestion<String>> converter = new SuggestionConverter<>(Suggestion::getString,
                Suggestion::getScore, Suggestion::getPayload);
        Sugadd<String, String, Suggestion<String>> sugadd = new Sugadd<>(t -> key, converter);
        OperationItemWriter<String, String, Suggestion<String>> writer = new OperationItemWriter<>(sourceClient,
                StringCodec.UTF8, sugadd);
        run(testInfo, reader, writer);
        RedisModulesCommands<String, String> sync = sourceConnection.sync();
        assertEquals(1, sync.dbsize());
        assertEquals(values.size(), sync.ftSuglen(key));
    }

    @Test
    void writeSugIncr(TestInfo testInfo) throws Exception {
        String key = "sugaddIncr";
        List<Suggestion<String>> values = new ArrayList<>();
        for (int index = 0; index < 100; index++) {
            values.add(Suggestion.string("word" + index).score(index + 1).payload("payload" + index).build());
        }
        ListItemReader<Suggestion<String>> reader = new ListItemReader<>(values);
        SuggestionConverter<String, Suggestion<String>> converter = new SuggestionConverter<>(Suggestion::getString,
                Suggestion::getScore, Suggestion::getPayload);
        SugaddIncr<String, String, Suggestion<String>> sugadd = new SugaddIncr<>(t -> key, converter);
        OperationItemWriter<String, String, Suggestion<String>> writer = new OperationItemWriter<>(sourceClient,
                StringCodec.UTF8, sugadd);
        run(testInfo, reader, writer);
        RedisModulesCommands<String, String> sync = sourceConnection.sync();
        assertEquals(1, sync.dbsize());
        assertEquals(values.size(), sync.ftSuglen(key));
    }

}
