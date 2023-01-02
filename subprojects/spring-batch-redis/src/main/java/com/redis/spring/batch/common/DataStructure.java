package com.redis.spring.batch.common;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.redis.lettucemod.timeseries.Sample;

import io.lettuce.core.ScoredValue;
import io.lettuce.core.StreamMessage;

public class DataStructure<K> extends KeyTtlValue<K> {

	private Type type;

	/**
	 * Redis value. Null if key does not exist
	 * 
	 */
	private Object value;

	public Object getValue() {
		return value;
	}

	public void setValue(Object value) {
		this.value = value;
	}

	public Type getType() {
		return type;
	}

	public void setType(Type type) {
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

	public enum Type {

		NONE("none"), UNKNOWN, SET("set"), LIST("list"), ZSET("zset"), STREAM("stream"), STRING("string"), HASH("hash"),
		JSON("ReJSON-RL"), TIMESERIES("TSDB-TYPE");

		private static final Map<String, Type> TYPES = Stream.of(Type.values())
				.collect(Collectors.toMap(t -> t.string.toLowerCase(), Function.identity()));

		private String string;

		Type() {
			this.string = name();
		}

		Type(String string) {
			this.string = string;
		}

		/**
		 * 
		 * @return The Redis type name
		 */
		public String getString() {
			return string;
		}

		public static Type of(String string) {
			if (string == null) {
				throw new NullPointerException("Name is null");
			}
			return TYPES.getOrDefault(string.toLowerCase(), UNKNOWN);
		}

	}

	public static <K> DataStructure<K> of(K key, Type type, Object value) {
		DataStructure<K> dataStructure = new DataStructure<>();
		dataStructure.setKey(key);
		dataStructure.setType(type);
		dataStructure.setValue(value);
		return dataStructure;
	}

	public static <K, V> DataStructure<K> hash(K key, Map<K, V> value) {
		return DataStructure.of(key, Type.HASH, value);
	}

	public static <K, V> DataStructure<K> string(K key, V value) {
		return DataStructure.of(key, Type.STRING, value);
	}

	public static <K, V> DataStructure<K> set(K key, Collection<V> value) {
		return DataStructure.of(key, Type.SET, value);
	}

	public static <K, V> DataStructure<K> zset(K key, Collection<ScoredValue<V>> value) {
		return DataStructure.of(key, Type.ZSET, value);
	}

	public static <K, V> DataStructure<K> list(K key, Collection<V> value) {
		return DataStructure.of(key, Type.LIST, value);
	}

	public static <K, V> DataStructure<K> stream(K key, Collection<StreamMessage<K, V>> value) {
		return DataStructure.of(key, Type.STREAM, value);
	}

	public static <K, V> DataStructure<K> json(K key, V value) {
		return DataStructure.of(key, Type.JSON, value);
	}

	public static <K> DataStructure<K> timeSeries(K key, Collection<Sample> value) {
		return DataStructure.of(key, Type.TIMESERIES, value);
	}

	public void setTypeString(String string) {
		setType(Type.of(string));
	}

}
