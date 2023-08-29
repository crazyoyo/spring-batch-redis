package com.redis.spring.batch.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

import org.springframework.batch.item.ItemStreamException;
import org.springframework.util.ClassUtils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.redis.lettucemod.timeseries.Sample;
import com.redis.spring.batch.KeyValue;
import com.redis.spring.batch.util.GeneratorOptions.CollectionOptions;
import com.redis.spring.batch.util.GeneratorOptions.MapOptions;
import com.redis.spring.batch.util.GeneratorOptions.Type;

import io.lettuce.core.ScoredValue;
import io.lettuce.core.StreamMessage;

public class GeneratorItemReader extends AbstractCountingItemReader<KeyValue<String>> {

    private static final int LEFT_LIMIT = 48; // numeral '0'

    private static final int RIGHT_LIMIT = 122; // letter 'z'

    private final ObjectMapper mapper = new ObjectMapper();

    private static final Random random = new Random();

    private GeneratorOptions options = new GeneratorOptions();

    private boolean open;

    public GeneratorItemReader() {
        setName(ClassUtils.getShortName(getClass()));
    }

    public GeneratorOptions getOptions() {
        return options;
    }

    public void setOptions(GeneratorOptions options) {
        this.options = options;
    }

    private String key() {
        int index = index(options.getKeyRange());
        return options.getKeyspace() + ":" + index;
    }

    private int index(IntRange range) {
        return range.getMin() + index() % (range.getMax() - range.getMin() + 1);
    }

    private Object value(KeyValue<String> ds) throws JsonProcessingException {
        switch (ds.getType()) {
            case KeyValue.HASH:
                return map(options.getHashOptions());
            case KeyValue.LIST:
                return members(options.getListOptions());
            case KeyValue.SET:
                return new HashSet<>(members(options.getSetOptions()));
            case KeyValue.STREAM:
                return streamMessages();
            case KeyValue.STRING:
                return string(options.getStringOptions().getLength());
            case KeyValue.ZSET:
                return zset();
            case KeyValue.JSON:
                return mapper.writeValueAsString(map(options.getJsonOptions()));
            case KeyValue.TIMESERIES:
                return samples();
            default:
                return null;
        }
    }

    private List<Sample> samples() {
        List<Sample> samples = new ArrayList<>();
        int size = randomInt(options.getTimeSeriesOptions().getSampleCount());
        for (int index = 0; index < size; index++) {
            long time = options.getTimeSeriesOptions().getStartTime() + index() + index;
            samples.add(Sample.of(time, random.nextDouble()));
        }
        return samples;
    }

    private List<ScoredValue<String>> zset() {
        return members(options.getZsetOptions()).stream().map(this::scoredValue).collect(Collectors.toList());
    }

    private ScoredValue<String> scoredValue(String value) {
        double score = randomDouble(options.getZsetOptions().getScore());
        return ScoredValue.just(score, value);
    }

    private Collection<StreamMessage<String, String>> streamMessages() {
        String key = key();
        Collection<StreamMessage<String, String>> messages = new ArrayList<>();
        for (int elementIndex = 0; elementIndex < randomInt(options.getStreamOptions().getMessageCount()); elementIndex++) {
            messages.add(new StreamMessage<>(key, null, map(options.getStreamOptions().getBodyOptions())));
        }
        return messages;
    }

    private Map<String, String> map(MapOptions options) {
        Map<String, String> hash = new HashMap<>();
        for (int index = 0; index < randomInt(options.getFieldCount()); index++) {
            int fieldIndex = index + 1;
            hash.put("field" + fieldIndex, string(options.getFieldLength()));
        }
        return hash;
    }

    private String string(IntRange range) {
        int length = range.getMin() + random.nextInt((range.getMax() - range.getMin()) + 1);
        return randomString(length);
    }

    public static String randomString(int length) {
        return random.ints(LEFT_LIMIT, RIGHT_LIMIT + 1).filter(i -> (i <= 57 || i >= 65) && (i <= 90 || i >= 97)).limit(length)
                .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append).toString();
    }

    private List<String> members(CollectionOptions options) {
        List<String> members = new ArrayList<>();
        for (int index = 0; index < randomInt(options.getCardinality()); index++) {
            int memberId = options.getMemberRange().getMin()
                    + index % (options.getMemberRange().getMax() - options.getMemberRange().getMin() + 1);
            members.add(String.valueOf(memberId));
        }
        return members;
    }

    private int randomInt(IntRange range) {
        if (range.getMin() == range.getMax()) {
            return range.getMin();
        }
        return ThreadLocalRandom.current().nextInt(range.getMin(), range.getMax());
    }

    private double randomDouble(DoubleRange range) {
        if (range.getMin() == range.getMax()) {
            return range.getMin();
        }
        return ThreadLocalRandom.current().nextDouble(range.getMin(), range.getMax());
    }

    @Override
    protected KeyValue<String> doRead() {
        KeyValue<String> ds = new KeyValue<>();
        Type type = options.getTypes().get(index() % options.getTypes().size());
        ds.setType(typeString(type));
        ds.setKey(key());
        Object value;
        try {
            value = value(ds);
        } catch (JsonProcessingException e) {
            throw new ItemStreamException("Could not read value", e);
        }
        ds.setValue(value);
        if (options.getExpiration() != null) {
            ds.setTtl(System.currentTimeMillis() + randomInt(options.getExpiration()));
        }
        return ds;
    }

    private String typeString(Type type) {
        switch (type) {
            case HASH:
                return KeyValue.HASH;
            case JSON:
                return KeyValue.JSON;
            case LIST:
                return KeyValue.LIST;
            case SET:
                return KeyValue.SET;
            case STREAM:
                return KeyValue.STREAM;
            case STRING:
                return KeyValue.STRING;
            case TIMESERIES:
                return KeyValue.TIMESERIES;
            case ZSET:
                return KeyValue.ZSET;
            default:
                return KeyValue.NONE;
        }
    }

    private int index() {
        return getCurrentItemCount() - 1;
    }

    @Override
    protected void doOpen() {
        this.open = true;
    }

    @Override
    protected void doClose() {
        this.open = false;
    }

    public boolean isOpen() {
        return open;
    }

}
