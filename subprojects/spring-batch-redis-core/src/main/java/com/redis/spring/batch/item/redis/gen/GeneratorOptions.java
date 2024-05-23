package com.redis.spring.batch.item.redis.gen;

import java.util.Arrays;
import java.util.List;

import com.redis.spring.batch.Range;
import com.redis.spring.batch.item.redis.common.DataType;

public class GeneratorOptions {

	public static final String DEFAULT_KEYSPACE = "gen";
	public static final String DEFAULT_KEY_SEPARATOR = ":";
	public static final Range DEFAULT_KEY_RANGE = Range.from(1);

	public static List<DataType> defaultTypes() {
		return Arrays.asList(DataType.HASH, DataType.JSON, DataType.LIST, DataType.SET, DataType.STREAM,
				DataType.STRING, DataType.TIMESERIES, DataType.ZSET);
	}

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
	private List<DataType> types = defaultTypes();

	public String getKeySeparator() {
		return keySeparator;
	}

	public void setKeySeparator(String keySeparator) {
		this.keySeparator = keySeparator;
	}

	public String getKeyspace() {
		return keyspace;
	}

	public void setKeyspace(String keyspace) {
		this.keyspace = keyspace;
	}

	public Range getKeyRange() {
		return keyRange;
	}

	public void setKeyRange(Range keyRange) {
		this.keyRange = keyRange;
	}

	public Range getExpiration() {
		return expiration;
	}

	public void setExpiration(Range expiration) {
		this.expiration = expiration;
	}

	public MapOptions getHashOptions() {
		return hashOptions;
	}

	public void setHashOptions(MapOptions hashOptions) {
		this.hashOptions = hashOptions;
	}

	public StreamOptions getStreamOptions() {
		return streamOptions;
	}

	public void setStreamOptions(StreamOptions streamOptions) {
		this.streamOptions = streamOptions;
	}

	public TimeSeriesOptions getTimeSeriesOptions() {
		return timeSeriesOptions;
	}

	public void setTimeSeriesOptions(TimeSeriesOptions timeSeriesOptions) {
		this.timeSeriesOptions = timeSeriesOptions;
	}

	public MapOptions getJsonOptions() {
		return jsonOptions;
	}

	public void setJsonOptions(MapOptions jsonOptions) {
		this.jsonOptions = jsonOptions;
	}

	public CollectionOptions getListOptions() {
		return listOptions;
	}

	public void setListOptions(CollectionOptions listOptions) {
		this.listOptions = listOptions;
	}

	public CollectionOptions getSetOptions() {
		return setOptions;
	}

	public void setSetOptions(CollectionOptions setOptions) {
		this.setOptions = setOptions;
	}

	public StringOptions getStringOptions() {
		return stringOptions;
	}

	public void setStringOptions(StringOptions stringOptions) {
		this.stringOptions = stringOptions;
	}

	public ZsetOptions getZsetOptions() {
		return zsetOptions;
	}

	public void setZsetOptions(ZsetOptions zsetOptions) {
		this.zsetOptions = zsetOptions;
	}

	public List<DataType> getTypes() {
		return types;
	}

	public void setTypes(List<DataType> types) {
		this.types = types;
	}

	public void setTypes(DataType... types) {
		setTypes(Arrays.asList(types));
	}
}
