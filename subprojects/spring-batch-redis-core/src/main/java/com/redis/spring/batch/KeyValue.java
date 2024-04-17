package com.redis.spring.batch;

import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class KeyValue<K> {

	public static final long TTL_NO_KEY = -2;

	private K key;
	private Type type;
	private Object value;

	/**
	 * Expiration POSIX time in milliseconds for this key.
	 *
	 */
	private long ttl;

	/**
	 * Number of bytes that this key and its value require to be stored in Redis
	 * RAM. 0 means no memory usage information is available.
	 */
	private long mem;

	public enum Type {

		HASH("hash"), JSON("ReJSON-RL"), LIST("list"), SET("set"), STREAM("stream"), STRING("string"),
		TIMESERIES("TSDB-TYPE"), ZSET("zset");

		private static final Function<Type, String> DATATYPE_STRING = Type::getCode;
		private static final UnaryOperator<String> TO_LOWER_CASE = String::toLowerCase;
		private static final Map<String, Type> TYPE_MAP = Stream.of(Type.values())
				.collect(Collectors.toMap(DATATYPE_STRING.andThen(TO_LOWER_CASE), Function.identity()));

		private final String code;

		private Type(String string) {
			this.code = string;
		}

		public String getCode() {
			return code;
		}

		public static Type of(String string) {
			return TYPE_MAP.get(TO_LOWER_CASE.apply(string));
		}

	}

	public K getKey() {
		return key;
	}

	public void setKey(K key) {
		this.key = key;
	}

	public Type getType() {
		return type;
	}

	public void setType(Type type) {
		this.type = type;
	}

	public Object getValue() {
		return value;
	}

	public void setValue(Object value) {
		this.value = value;
	}

	public long getTtl() {
		return ttl;
	}

	public void setTtl(long ttl) {
		this.ttl = ttl;
	}

	public long getMem() {
		return mem;
	}

	public void setMem(long bytes) {
		this.mem = bytes;
	}

	public boolean exists() {
		return type != null && ttl != KeyValue.TTL_NO_KEY;
	}

	@Override
	public int hashCode() {
		return Objects.hash(key, mem, ttl, type, value);
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		KeyValue<?> other = (KeyValue<?>) obj;
		return Objects.equals(key, other.key) && mem == other.mem && ttl == other.ttl && type == other.type
				&& Objects.equals(value, other.value);
	}

	@Override
	public String toString() {
		return "KeyValue [key=" + key + ", type=" + type + ", value=" + value + ", ttl=" + ttl + ", memoryUsage=" + mem
				+ "]";
	}

}
