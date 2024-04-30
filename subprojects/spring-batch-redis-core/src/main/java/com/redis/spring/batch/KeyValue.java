package com.redis.spring.batch;

import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.springframework.util.StringUtils;

public class KeyValue<K, T> {

	public enum DataType {

		NONE("none"), HASH("hash"), JSON("ReJSON-RL"), LIST("list"), SET("set"), STREAM("stream"), STRING("string"),
		TIMESERIES("TSDB-TYPE"), ZSET("zset");
		
		private static final Map<String, DataType> TYPE_MAP = Stream.of(DataType.values())
				.collect(Collectors.toMap(t -> t.getString().toLowerCase(), Function.identity()));

		private final String string;

		private DataType(String string) {
			this.string = string;
		}

		public String getString() {
			return string;
		}

		public static DataType of(String string) {
			return TYPE_MAP.get(string.toLowerCase());
		}

	}

	public static final long TTL_NO_KEY = -2;

	private K key;
	private String type;
	private T value;
	/**
	 * Expiration POSIX time in milliseconds for this key.
	 */
	private long ttl;

	public KeyValue() {

	}

	public KeyValue(KeyValue<K, T> other) {
		this.key = other.key;
		this.ttl = other.ttl;
		this.type = other.type;
		this.value = other.value;
	}

	public K getKey() {
		return key;
	}

	public void setKey(K key) {
		this.key = key;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public T getValue() {
		return value;
	}

	public void setValue(T value) {
		this.value = value;
	}

	public long getTtl() {
		return ttl;
	}

	public void setTtl(long ttl) {
		this.ttl = ttl;
	}

	@Override
	public int hashCode() {
		return Objects.hash(key, ttl, type, value);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		KeyValue other = (KeyValue) obj;
		return Objects.equals(key, other.key) && ttl == other.ttl && Objects.equals(type, other.type)
				&& Objects.equals(value, other.value);
	}

	public static boolean exists(KeyValue<?, ?> kv) {
		return kv.getTtl() != TTL_NO_KEY && type(kv) != DataType.NONE;
	}

	public static boolean hasTtl(KeyValue<?, ?> kv) {
		return kv.getTtl() > 0;
	}

	public static boolean hasValue(KeyValue<?, ?> keyValue) {
		return keyValue.getValue() != null;
	}

	public static boolean hasType(KeyValue<?, ?> keyValue) {
		return StringUtils.hasLength(keyValue.getType());
	}

	public static DataType type(KeyValue<?, ?> keyValue) {
		if (hasType(keyValue)) {
			return DataType.of(keyValue.getType());
		}
		return null;
	}

}
