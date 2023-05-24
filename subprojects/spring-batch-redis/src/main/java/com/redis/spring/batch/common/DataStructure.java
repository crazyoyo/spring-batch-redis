package com.redis.spring.batch.common;

import java.util.Objects;

public class DataStructure<K> extends KeyTtlValue<K> {

	public static final String NONE = "none";
	public static final String SET = "set";
	public static final String LIST = "list";
	public static final String ZSET = "zset";
	public static final String STREAM = "stream";
	public static final String STRING = "string";
	public static final String HASH = "hash";
	public static final String JSON = "ReJSON-RL";
	public static final String TIMESERIES = "TSDB-TYPE";

	private String type = NONE;

	/**
	 * Redis value. Null if key does not exist
	 */
	private Object value;

	@SuppressWarnings("unchecked")
	public <T> T getValue() {
		return (T) value;
	}

	public void setValue(Object value) {
		this.value = value;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	@Override
	public String toString() {
		return "DataStructure [type=" + type + ", key=" + getKey() + ", value=" + value + ", ttl=" + getTtl() + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + Objects.hash(type, value);
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (getClass() != obj.getClass())
			return false;
		DataStructure<?> other = (DataStructure<?>) obj;
		return type == other.type && Objects.equals(value, other.value);
	}

	public static boolean isNone(DataStructure<?> item) {
		return NONE.equals(item.getType());
	}

}
