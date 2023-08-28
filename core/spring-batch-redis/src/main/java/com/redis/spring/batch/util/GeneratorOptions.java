package com.redis.spring.batch.util;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class GeneratorOptions {

    public enum Type {
        HASH, STRING, LIST, SET, ZSET, JSON, STREAM, TIMESERIES
    }

    public static final String DEFAULT_KEYSPACE = "gen";

    private static final Type[] DEFAULT_TYPES = { Type.HASH, Type.LIST, Type.SET, Type.STREAM, Type.STRING, Type.ZSET };

    public static final IntRange DEFAULT_KEY_RANGE = IntRange.from(1);

    public static final IntRange DEFAULT_COLLECTION_MEMBER_RANGE = IntRange.between(1, 100);

    public static final IntRange DEFAULT_COLLECTION_CARDINALITY = IntRange.is(100);

    public static final IntRange DEFAULT_STRING_LENGTH = IntRange.is(100);

    public static final IntRange DEFAULT_FIELD_COUNT = IntRange.is(10);

    public static final IntRange DEFAULT_FIELD_LENGTH = IntRange.is(100);

    public static final IntRange DEFAULT_STREAM_MESSAGE_COUNT = IntRange.is(10);

    public static final DoubleRange DEFAULT_ZSET_SCORE = DoubleRange.between(0, 100);

    public static final IntRange DEFAULT_TIMESERIES_SAMPLE_COUNT = IntRange.is(10);

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

    private List<Type> types = defaultTypes();

    public static List<Type> defaultTypes() {
        return Stream.of(DEFAULT_TYPES).collect(Collectors.toList());
    }

    public void setKeyRange(IntRange keyRange) {
        this.keyRange = keyRange;
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

    public List<Type> getTypes() {
        return types;
    }

    public void setExpiration(IntRange expiration) {
        this.expiration = expiration;
    }

    public void setHashOptions(MapOptions hashOptions) {
        this.hashOptions = hashOptions;
    }

    public void setStreamOptions(StreamOptions streamOptions) {
        this.streamOptions = streamOptions;
    }

    public void setJsonOptions(MapOptions jsonOptions) {
        this.jsonOptions = jsonOptions;
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

    public void setTypes(Type... types) {
        setTypes(Arrays.asList(types));
    }

    public void setTypes(List<Type> types) {
        this.types = types;
    }

    public static class ZsetOptions extends CollectionOptions {

        private DoubleRange score = DEFAULT_ZSET_SCORE;

        public DoubleRange getScore() {
            return score;
        }

        public void setScore(DoubleRange score) {
            this.score = score;
        }

    }

    public static class TimeSeriesOptions {

        private IntRange sampleCount = DEFAULT_TIMESERIES_SAMPLE_COUNT;

        private long startTime;

        public IntRange getSampleCount() {
            return sampleCount;
        }

        public void setSampleCount(IntRange sampleCount) {
            this.sampleCount = sampleCount;
        }

        public long getStartTime() {
            return startTime;
        }

        public void setStartTime(long startTime) {
            this.startTime = startTime;
        }

    }

    public static class StreamOptions {

        private IntRange messageCount = DEFAULT_STREAM_MESSAGE_COUNT;

        private MapOptions bodyOptions = new MapOptions();

        public IntRange getMessageCount() {
            return messageCount;
        }

        public void setMessageCount(IntRange count) {
            this.messageCount = count;
        }

        public MapOptions getBodyOptions() {
            return bodyOptions;
        }

        public void setBodyOptions(MapOptions options) {
            this.bodyOptions = options;
        }

    }

    public static class CollectionOptions {

        private IntRange memberRange = DEFAULT_COLLECTION_MEMBER_RANGE;

        private IntRange cardinality = DEFAULT_COLLECTION_CARDINALITY;

        public IntRange getMemberRange() {
            return memberRange;
        }

        public void setMemberRange(IntRange range) {
            this.memberRange = range;
        }

        public IntRange getCardinality() {
            return cardinality;
        }

        public void setCardinality(IntRange cardinality) {
            this.cardinality = cardinality;
        }

    }

    public static class StringOptions {

        private IntRange length = DEFAULT_STRING_LENGTH;

        public IntRange getLength() {
            return length;
        }

        public void setLength(IntRange length) {
            this.length = length;
        }

    }

    public static class MapOptions {

        private IntRange fieldCount = DEFAULT_FIELD_COUNT;

        private IntRange fieldLength = DEFAULT_FIELD_LENGTH;

        public IntRange getFieldCount() {
            return fieldCount;
        }

        public void setFieldCount(IntRange count) {
            this.fieldCount = count;
        }

        public IntRange getFieldLength() {
            return fieldLength;
        }

        public void setFieldLength(IntRange length) {
            this.fieldLength = length;
        }

    }

}
