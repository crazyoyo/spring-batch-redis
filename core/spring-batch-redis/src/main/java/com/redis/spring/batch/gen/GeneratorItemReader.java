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
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.util.ClassUtils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.redis.lettucemod.timeseries.Sample;
import com.redis.spring.batch.common.DataStructureType;
import com.redis.spring.batch.common.Range;
import com.redis.spring.batch.common.Struct;

import io.lettuce.core.ScoredValue;
import io.lettuce.core.StreamMessage;

public class GeneratorItemReader extends AbstractItemCountingItemStreamItemReader<Struct<String>> {

    public static final String DEFAULT_KEYSPACE = "gen";

    public static final String DEFAULT_KEY_SEPARATOR = ":";

    private static final DataStructureType[] DEFAULT_TYPES = { DataStructureType.HASH, DataStructureType.LIST, DataStructureType.SET, DataStructureType.STREAM, DataStructureType.STRING, DataStructureType.ZSET };

    public static final Range DEFAULT_KEY_RANGE = Range.from(1);

    private static final int LEFT_LIMIT = 48; // numeral '0'

    private static final int RIGHT_LIMIT = 122; // letter 'z'

    private final ObjectMapper mapper = new ObjectMapper();

    private static final Random random = new Random();

    private String keySeparator = DEFAULT_KEY_SEPARATOR;

    private String keyspace = DEFAULT_KEYSPACE;

    private Range keyRange = DEFAULT_KEY_RANGE;

    private Range expiration;

    private MapOptions hashOptions = new MapOptions();

    private StreamOptions streamOptions = new StreamOptions();

    private TimeSeriesOptions timeSeriesOptions = new TimeSeriesOptions();

    private MapOptions jsonOptions = new MapOptions();

    private CollectionOptions listOptions = new CollectionOptions();

    private CollectionOptions setOptions = new CollectionOptions();

    private StringOptions stringOptions = new StringOptions();

    private ZsetOptions zsetOptions = new ZsetOptions();

    private List<DataStructureType> types = defaultTypes();

    private boolean open;

    public GeneratorItemReader() {
        setName(ClassUtils.getShortName(getClass()));
    }

    public static List<DataStructureType> defaultTypes() {
        return Arrays.asList(DEFAULT_TYPES);
    }

    public String getKeySeparator() {
        return keySeparator;
    }

    public void setKeySeparator(String keySeparator) {
        this.keySeparator = keySeparator;
    }

    public void setKeyRange(Range range) {
        this.keyRange = range;
    }

    public Range getKeyRange() {
        return keyRange;
    }

    public Range getExpiration() {
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

    public List<DataStructureType> getTypes() {
        return types;
    }

    public void setExpiration(Range range) {
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

    public void setTypes(DataStructureType... types) {
        setTypes(Arrays.asList(types));
    }

    public void setTypes(List<DataStructureType> types) {
        this.types = types;
    }

    private String key() {
        StringBuilder builder = new StringBuilder();
        builder.append(keyspace);
        builder.append(keySeparator);
        builder.append(index(keyRange));
        return builder.toString();
    }

    private int index(Range range) {
        return range.getMin() + index() % (range.getMax() - range.getMin() + 1);
    }

    private Object value(DataStructureType type) throws JsonProcessingException {
        switch (type) {
            case HASH:
                return map(hashOptions);
            case LIST:
                return members(listOptions);
            case SET:
                return new HashSet<>(members(setOptions));
            case STREAM:
                return streamMessages();
            case STRING:
                return string(stringOptions.getLength());
            case ZSET:
                return zset();
            case JSON:
                return mapper.writeValueAsString(map(jsonOptions));
            case TIMESERIES:
                return samples();
            default:
                return null;
        }
    }

    private List<Sample> samples() {
        List<Sample> samples = new ArrayList<>();
        long size = randomLong(timeSeriesOptions.getSampleCount());
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
        for (int elementIndex = 0; elementIndex < randomLong(streamOptions.getMessageCount()); elementIndex++) {
            messages.add(new StreamMessage<>(key, null, map(streamOptions.getBodyOptions())));
        }
        return messages;
    }

    private Map<String, String> map(MapOptions options) {
        Map<String, String> hash = new HashMap<>();
        for (int index = 0; index < randomLong(options.getFieldCount()); index++) {
            int fieldIndex = index + 1;
            hash.put("field" + fieldIndex, string(options.getFieldLength()));
        }
        return hash;
    }

    private String string(Range range) {
        long length = randomLong(range);
        return string(length);
    }

    public static String string(long length) {
        return random.ints(LEFT_LIMIT, RIGHT_LIMIT + 1).filter(i -> (i <= 57 || i >= 65) && (i <= 90 || i >= 97)).limit(length)
                .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append).toString();
    }

    private List<String> members(CollectionOptions options) {
        List<String> members = new ArrayList<>();
        for (int index = 0; index < randomLong(options.getMemberCount()); index++) {
            long memberId = options.getMemberRange().getMin()
                    + index % (options.getMemberRange().getMax() - options.getMemberRange().getMin() + 1);
            members.add(String.valueOf(memberId));
        }
        return members;
    }

    private long randomLong(Range range) {
        if (range.getMin() == range.getMax()) {
            return range.getMin();
        }
        return ThreadLocalRandom.current().nextLong(range.getMin(), range.getMax());
    }

    private double randomDouble(Range range) {
        if (range.getMin() == range.getMax()) {
            return range.getMin();
        }
        return ThreadLocalRandom.current().nextDouble(range.getMin(), range.getMax());
    }

    @Override
    protected Struct<String> doRead() {
        Struct<String> struct = new Struct<>();
        struct.setKey(key());
        DataStructureType type = types.get(index() % types.size());
        struct.setType(type);
        try {
            struct.setValue(value(type));
        } catch (JsonProcessingException e) {
            throw new ItemStreamException("Could not read value", e);
        }
        if (expiration != null) {
            struct.setTtl(ttl());
        }
        return struct;
    }

    private long ttl() {
        return System.currentTimeMillis() + randomLong(expiration);
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
