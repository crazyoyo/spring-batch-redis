package com.redis.spring.batch.gen;

import java.util.ArrayList;
import java.util.Arrays;
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
import com.redis.spring.batch.util.AbstractCountingItemReader;
import com.redis.spring.batch.util.DoubleRange;
import com.redis.spring.batch.util.IntRange;

import io.lettuce.core.ScoredValue;
import io.lettuce.core.StreamMessage;

public class GeneratorItemReader extends AbstractCountingItemReader<KeyValue<String>> {

    public static final String DEFAULT_KEYSPACE = "gen";

    private static final DataType[] DEFAULT_TYPES = { DataType.HASH, DataType.LIST, DataType.SET, DataType.STREAM, DataType.STRING, DataType.ZSET };

    public static final IntRange DEFAULT_KEY_RANGE = IntRange.from(1);

    private static final int LEFT_LIMIT = 48; // numeral '0'

    private static final int RIGHT_LIMIT = 122; // letter 'z'

    private final ObjectMapper mapper = new ObjectMapper();

    private static final Random random = new Random();

    private IntRange keyRange = DEFAULT_KEY_RANGE;

    private IntRange expiration;

    private MapOptions hashOptions = new MapOptions();

    private StreamOptions streamOptions = new StreamOptions();

    private TimeSeriesOptions timeSeriesOptions = new TimeSeriesOptions();

    private MapOptions jsonOptions = new MapOptions();

    private CollectionOptions listOptions = new CollectionOptions();

    private CollectionOptions setOptions = new CollectionOptions();

    private StringOptions stringOptions = new StringOptions();

    private ZsetOptions zsetOptions = new ZsetOptions();

    private String keyspace = DEFAULT_KEYSPACE;

    private List<DataType> types = defaultTypes();

    private boolean open;

    public GeneratorItemReader() {
        setName(ClassUtils.getShortName(getClass()));
    }

    public static List<DataType> defaultTypes() {
        return Arrays.asList(DEFAULT_TYPES);
    }

    public void setKeyRange(IntRange range) {
        this.keyRange = range;
    }

    public IntRange getKeyRange() {
        return keyRange;
    }

    public IntRange getExpiration() {
        return expiration;
    }

    public MapOptions getHashOptions() {
        return hashOptions;
    }

    public StreamOptions getStreamOptions() {
        return streamOptions;
    }

    public TimeSeriesOptions getTimeSeriesOptions() {
        return timeSeriesOptions;
    }

    public MapOptions getJsonOptions() {
        return jsonOptions;
    }

    public CollectionOptions getListOptions() {
        return listOptions;
    }

    public CollectionOptions getSetOptions() {
        return setOptions;
    }

    public StringOptions getStringOptions() {
        return stringOptions;
    }

    public ZsetOptions getZsetOptions() {
        return zsetOptions;
    }

    public String getKeyspace() {
        return keyspace;
    }

    public List<DataType> getTypes() {
        return types;
    }

    public void setExpiration(IntRange range) {
        this.expiration = range;
    }

    public void setHashOptions(MapOptions options) {
        this.hashOptions = options;
    }

    public void setStreamOptions(StreamOptions options) {
        this.streamOptions = options;
    }

    public void setJsonOptions(MapOptions options) {
        this.jsonOptions = options;
    }

    public void setTimeSeriesOptions(TimeSeriesOptions options) {
        this.timeSeriesOptions = options;
    }

    public void setListOptions(CollectionOptions options) {
        this.listOptions = options;
    }

    public void setSetOptions(CollectionOptions options) {
        this.setOptions = options;
    }

    public void setZsetOptions(ZsetOptions options) {
        this.zsetOptions = options;
    }

    public void setStringOptions(StringOptions options) {
        this.stringOptions = options;
    }

    public void setKeyspace(String keyspace) {
        this.keyspace = keyspace;
    }

    public void setTypes(DataType... types) {
        setTypes(Arrays.asList(types));
    }

    public void setTypes(List<DataType> types) {
        this.types = types;
    }

    private String key() {
        int index = index(keyRange);
        return keyspace + ":" + index;
    }

    private int index(IntRange range) {
        return range.getMin() + index() % (range.getMax() - range.getMin() + 1);
    }

    private Object value(KeyValue<String> ds) throws JsonProcessingException {
        switch (ds.getType()) {
            case KeyValue.HASH:
                return map(hashOptions);
            case KeyValue.LIST:
                return members(listOptions);
            case KeyValue.SET:
                return new HashSet<>(members(setOptions));
            case KeyValue.STREAM:
                return streamMessages();
            case KeyValue.STRING:
                return string(stringOptions.getLength());
            case KeyValue.ZSET:
                return zset();
            case KeyValue.JSON:
                return mapper.writeValueAsString(map(jsonOptions));
            case KeyValue.TIMESERIES:
                return samples();
            default:
                return null;
        }
    }

    private List<Sample> samples() {
        List<Sample> samples = new ArrayList<>();
        int size = randomInt(timeSeriesOptions.getSampleCount());
        for (int index = 0; index < size; index++) {
            long time = timeSeriesStartTime() + index() + index;
            samples.add(Sample.of(time, random.nextDouble()));
        }
        return samples;
    }

    private long timeSeriesStartTime() {
        if (timeSeriesOptions.getStartTime() == null) {
            return System.currentTimeMillis();
        }
        return timeSeriesOptions.getStartTime().toEpochMilli();
    }

    private List<ScoredValue<String>> zset() {
        return members(zsetOptions).stream().map(this::scoredValue).collect(Collectors.toList());
    }

    private ScoredValue<String> scoredValue(String value) {
        double score = randomDouble(zsetOptions.getScore());
        return ScoredValue.just(score, value);
    }

    private Collection<StreamMessage<String, String>> streamMessages() {
        String key = key();
        Collection<StreamMessage<String, String>> messages = new ArrayList<>();
        for (int elementIndex = 0; elementIndex < randomInt(streamOptions.getMessageCount()); elementIndex++) {
            messages.add(new StreamMessage<>(key, null, map(streamOptions.getBodyOptions())));
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
        for (int index = 0; index < randomInt(options.getMemberCount()); index++) {
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
        DataType type = types.get(index() % types.size());
        ds.setType(typeString(type));
        ds.setKey(key());
        Object value;
        try {
            value = value(ds);
        } catch (JsonProcessingException e) {
            throw new ItemStreamException("Could not read value", e);
        }
        ds.setValue(value);
        if (expiration != null) {
            ds.setTtl(System.currentTimeMillis() + randomInt(expiration));
        }
        return ds;
    }

    private String typeString(DataType type) {
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
