package com.redis.spring.batch;

public class DataStructure<K> extends KeyValue<K, Object> {

	public static final String TYPE_SET = "set";
	public static final String TYPE_LIST = "list";
	public static final String TYPE_ZSET = "zset";
	public static final String TYPE_STREAM = "stream";
	public static final String TYPE_STRING = "string";
	public static final String TYPE_HASH = "hash";
	public static final String TYPE_NONE = "none";
	public static final String TYPE_JSON = "rejson-rl";
	public static final String TYPE_TIMESERIES = "tsdb-type";

	private String type;

	public DataStructure() {
	}

	public DataStructure(K key, String type) {
		super(key);
		this.type = type;
	}

	public DataStructure(K key, Object value, String type) {
		super(key, value);
		this.type = type;
	}

	public DataStructure(K key, Object value, Long absoluteTTL, String type) {
		super(key, value, absoluteTTL);
		this.type = type;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	@Override
	public String toString() {
		return "DataStructure [type=" + type + ", key=" + getKey() + ", value=" + getValue() + ", absoluteTTL="
				+ getAbsoluteTTL() + "]";
	}

}
