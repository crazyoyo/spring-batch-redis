package com.redis.spring.batch.test;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
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
import com.redis.lettucemod.api.sync.RedisModulesCommands;
import com.redis.lettucemod.search.IndexInfo;
import com.redis.lettucemod.search.Suggestion;
import com.redis.lettucemod.timeseries.AddOptions;
import com.redis.lettucemod.timeseries.DuplicatePolicy;
import com.redis.lettucemod.timeseries.RangeOptions;
import com.redis.lettucemod.timeseries.Sample;
import com.redis.lettucemod.timeseries.TimeRange;
import com.redis.lettucemod.util.RedisModulesUtils;
import com.redis.spring.batch.RedisItemWriter;
import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.common.KeyComparison;
import com.redis.spring.batch.common.KeyComparison.Status;
import com.redis.spring.batch.common.KeyComparisonItemReader;
import com.redis.spring.batch.common.Struct;
import com.redis.spring.batch.common.Struct.Type;
import com.redis.spring.batch.common.ToSampleFunction;
import com.redis.spring.batch.common.ToSuggestionFunction;
import com.redis.spring.batch.gen.GeneratorItemReader;
import com.redis.spring.batch.reader.KeyEvent;
import com.redis.spring.batch.reader.KeyspaceNotification;
import com.redis.spring.batch.reader.KeyspaceNotificationItemReader;
import com.redis.spring.batch.reader.LiveRedisItemReader;
import com.redis.spring.batch.util.BatchUtils;
import com.redis.spring.batch.util.CodecUtils;
import com.redis.spring.batch.writer.operation.JsonDel;
import com.redis.spring.batch.writer.operation.JsonSet;
import com.redis.spring.batch.writer.operation.Sugadd;
import com.redis.spring.batch.writer.operation.TsAdd;

import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.StringCodec;

abstract class ModulesTests extends ReplicationTests {

    private static final String JSON_BEER_1 = "[{\"id\":\"1\",\"brewery_id\":\"812\",\"name\":\"Hocus Pocus\",\"abv\":\"4.5\",\"ibu\":\"0\",\"srm\":\"0\",\"upc\":\"0\",\"filepath\":\"\",\"descript\":\"Our take on a classic summer ale.  A toast to weeds, rays, and summer haze.  A light, crisp ale for mowing lawns, hitting lazy fly balls, and communing with nature, Hocus Pocus is offered up as a summer sacrifice to clodless days.\\n\\nIts malty sweetness finishes tart and crisp and is best apprediated with a wedge of orange.\",\"add_user\":\"0\",\"last_mod\":\"2010-07-22 20:00:20 UTC\",\"style_name\":\"Light American Wheat Ale or Lager\",\"cat_name\":\"Other Style\"}]";

    private static final int BEER_COUNT = 1019;

    @Test
    void readKeyspaceNotifications(TestInfo testInfo) throws Exception {
        enableKeyspaceNotifications(client);
        KeyspaceNotificationItemReader<String> reader = new KeyspaceNotificationItemReader<>(client, StringCodec.UTF8);
        reader.open(new ExecutionContext());
        GeneratorItemReader gen = new GeneratorItemReader();
        gen.setMaxItemCount(100);
        gen.setTypes(Type.HASH, Type.LIST, Type.SET, Type.STREAM, Type.STRING, Type.ZSET, Type.TIMESERIES, Type.JSON);
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
    void liveReaderWithType(TestInfo info) throws Exception {
        enableKeyspaceNotifications(client);
        LiveRedisItemReader<String, String, Struct<String>> reader = liveStructReader(info, client);
        reader.setKeyType(Type.HASH.getString());
        reader.open(new ExecutionContext());
        GeneratorItemReader gen = new GeneratorItemReader();
        gen.setMaxItemCount(100);
        generate(info, gen);
        reader.open(new ExecutionContext());
        List<Struct<String>> keyValues = BatchUtils.readAll(reader);
        reader.close();
        Assertions.assertTrue(keyValues.stream().allMatch(v -> v.getType() == Type.HASH));
    }

    @Test
    void writeJsonSet(TestInfo testInfo) throws Exception {
        JsonSet<String, String, JsonNode> jsonSet = new JsonSet<>();
        jsonSet.setKeyFunction(n -> "beer:" + n.get("id").asText());
        jsonSet.setValue(JsonNode::toString);
        jsonSet.setPath(".");
        RedisItemWriter<String, String, JsonNode> writer = new RedisItemWriter<>(client, StringCodec.UTF8, jsonSet);
        IteratorItemReader<JsonNode> reader = new IteratorItemReader<>(Beers.jsonNodeIterator());
        run(testInfo, reader, writer);
        Assertions.assertEquals(BEER_COUNT, connection.sync().keys("beer:*").size());
        Assertions.assertEquals(new ObjectMapper().readTree(JSON_BEER_1),
                new ObjectMapper().readTree(connection.sync().jsonGet("beer:1", "$")));
    }

    @Test
    void writeJsonDel(TestInfo testInfo) throws Exception {
        GeneratorItemReader gen = new GeneratorItemReader();
        gen.setTypes(Type.JSON);
        gen.setMaxItemCount(DEFAULT_GENERATOR_COUNT);
        generate(testInfo, gen);
        JsonDel<String, String, Struct<String>> jsonDel = new JsonDel<>();
        jsonDel.setKeyFunction(Struct::getKey);
        run(testInfo, gen, new RedisItemWriter<>(client, StringCodec.UTF8, jsonDel));
        Assertions.assertEquals(0, connection.sync().dbsize());
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
        TsAdd<String, String, Sample> tsadd = new TsAdd<>();
        tsadd.setKey(key);
        tsadd.setSample(Function.identity());
        tsadd.setOptions(addOptions);
        RedisItemWriter<String, String, Sample> writer = new RedisItemWriter<>(client, StringCodec.UTF8, tsadd);
        run(testInfo, reader, writer);
        Assertions.assertEquals(count / 2,
                connection.sync().tsRange(key, TimeRange.unbounded(), RangeOptions.builder().build()).size(), 2);
    }

    @Test
    void beerIndex() throws Exception {
        Beers.populateIndex(connection);
        IndexInfo indexInfo = RedisModulesUtils.indexInfo(connection.sync().ftInfo(Beers.INDEX));
        Assertions.assertEquals(BEER_COUNT, indexInfo.getNumDocs());
    }

    @Test
    void tsComparator(TestInfo info) throws Exception {
        connection.sync().tsAdd("ts:1", Sample.of(123));
        KeyComparisonItemReader reader = comparisonReader(info);
        reader.setName(name(info));
        reader.open(new ExecutionContext());
        List<KeyComparison> comparisons = BatchUtils.readAll(reader);
        reader.close();
        Assertions.assertEquals(1, comparisons.stream().filter(c -> c.getStatus() == Status.MISSING).count());
    }

    @Test
    void testLuaTimeSeries() throws Exception {
        String key = "myts";
        Sample[] samples = { Sample.of(System.currentTimeMillis(), 1.1), Sample.of(System.currentTimeMillis() + 10, 2.2) };
        for (Sample sample : samples) {
            connection.sync().tsAdd(key, sample);
        }
        Function<List<? extends String>, List<Struct<String>>> reader = RedisItemReader.struct(client, StringCodec.UTF8)
                .toKeyValueFunction(pool);
        Struct<String> ds = reader.apply(Arrays.asList(key)).get(0);
        Assertions.assertEquals(key, ds.getKey());
        Assertions.assertEquals(Type.TIMESERIES, ds.getType());
        Assertions.assertEquals(Arrays.asList(samples), ds.getValue());
    }

    @Test
    void testLuaTimeSeriesByteArray() throws Exception {
        String key = "myts";
        Sample[] samples = { Sample.of(System.currentTimeMillis(), 1.1), Sample.of(System.currentTimeMillis() + 10, 2.2) };
        for (Sample sample : samples) {
            connection.sync().tsAdd(key, sample);
        }
        Function<List<? extends byte[]>, List<Struct<byte[]>>> function = RedisItemReader
                .struct(client, ByteArrayCodec.INSTANCE).toKeyValueFunction(bytePool);
        Function<String, byte[]> toByteArrayKeyFunction = CodecUtils.toByteArrayKeyFunction(StringCodec.UTF8);
        Struct<byte[]> ds = function.apply(Arrays.asList(toByteArrayKeyFunction.apply(key))).get(0);
        Assertions.assertArrayEquals(toByteArrayKeyFunction.apply(key), ds.getKey());
        Assertions.assertEquals(Type.TIMESERIES, ds.getType());
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
        ToSampleFunction<Sample> converter = new ToSampleFunction<>(Sample::getTimestamp, Sample::getValue);
        TsAdd<String, String, Sample> tsAdd = new TsAdd<>();
        tsAdd.setKey(key);
        tsAdd.setSample(converter);
        RedisItemWriter<String, String, Sample> writer = new RedisItemWriter<>(client, StringCodec.UTF8, tsAdd);
        run(testInfo, reader, writer);
        RedisModulesCommands<String, String> sync = connection.sync();
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
        Sugadd<String, String, Suggestion<String>> sugadd = new Sugadd<>();
        sugadd.setKey(key);
        sugadd.setSuggestion(new ToSuggestionFunction<>(Suggestion::getString, Suggestion::getScore, Suggestion::getPayload));
        RedisItemWriter<String, String, Suggestion<String>> writer = new RedisItemWriter<>(client, StringCodec.UTF8, sugadd);
        run(testInfo, reader, writer);
        RedisModulesCommands<String, String> sync = connection.sync();
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
        ToSuggestionFunction<String, Suggestion<String>> converter = new ToSuggestionFunction<>(Suggestion::getString,
                Suggestion::getScore, Suggestion::getPayload);
        Sugadd<String, String, Suggestion<String>> sugadd = new Sugadd<>();
        sugadd.setKey(key);
        sugadd.setSuggestion(converter);
        sugadd.setIncr(true);
        RedisItemWriter<String, String, Suggestion<String>> writer = new RedisItemWriter<>(client, StringCodec.UTF8, sugadd);
        run(testInfo, reader, writer);
        RedisModulesCommands<String, String> sync = connection.sync();
        assertEquals(1, sync.dbsize());
        assertEquals(values.size(), sync.ftSuglen(key));
    }

}
