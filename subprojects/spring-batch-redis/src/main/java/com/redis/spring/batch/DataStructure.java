package com.redis.spring.batch;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.springframework.util.Assert;

import com.redis.lettucemod.timeseries.Sample;

import io.lettuce.core.ScoredValue;
import io.lettuce.core.StreamMessage;

public class DataStructure<K> extends KeyValue<K, Object> {

	private Type type;
	private String typeString;

	public DataStructure() {
	}

	private DataStructure(Builder<K> builder) {
		super(builder);
		setType(builder.type);
	}

	public String getTypeString() {
		return typeString;
	}

	public void setTypeString(String string) {
		this.typeString = string;
		this.type = Type.of(string);
	}

	public Type getType() {
		return type;
	}

	public void setType(Type type) {
		Assert.notNull(type, "Type must not be null");
		this.typeString = type.getString();
		this.type = type;
	}

	@Override
	public String toString() {
		return "DataStructure [type=" + type + ", key=" + getKey() + ", value=" + getValue() + ", absoluteTTL="
				+ getTtl() + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + Objects.hash(type);
		return result;
	}

	@SuppressWarnings("rawtypes")
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (getClass() != obj.getClass())
			return false;
		DataStructure other = (DataStructure) obj;
		return Objects.equals(type, other.type);
	}

	public enum Type {

		NONE, UNKNOWN, SET, LIST, ZSET, STREAM, STRING, HASH, JSON("ReJSON-RL"), TIMESERIES("TSDB-TYPE");

		private static final Map<String, Type> TYPES = Stream.of(Type.values())
				.collect(Collectors.toMap(t -> t.string.toLowerCase(), t -> t));

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

	public static KeyBuilder type(Type type) {
		return new KeyBuilder(type);
	}

	public static <K, V> Builder<K> hash(K key, Map<K, V> value) {
		return new Builder<>(Type.HASH, key, value);
	}

	public static <K, V> Builder<K> string(K key, V value) {
		return new Builder<>(Type.STRING, key, value);
	}

	public static <K, V> Builder<K> set(K key, Collection<V> value) {
		return new Builder<>(Type.SET, key, value);
	}

	public static <K, V> Builder<K> zset(K key, Collection<ScoredValue<V>> value) {
		return new Builder<>(Type.ZSET, key, value);
	}

	public static <K, V> Builder<K> list(K key, Collection<V> value) {
		return new Builder<>(Type.LIST, key, value);
	}

	public static <K, V> Builder<K> stream(K key, Collection<StreamMessage<K, V>> value) {
		return new Builder<>(Type.STREAM, key, value);
	}

	public static <K, V> Builder<K> json(K key, V value) {
		return new Builder<>(Type.JSON, key, value);
	}

	public static <K> Builder<K> timeSeries(K key, Collection<Sample> value) {
		return new Builder<>(Type.TIMESERIES, key, value);
	}

	public static class KeyBuilder {

		private final Type type;

		public KeyBuilder(Type type) {
			this.type = type;
		}

		public <K> ValueBuilder<K> key(K key) {
			return new ValueBuilder<>(type, key);
		}
	}

	public static class ValueBuilder<K> {

		private final Type type;
		private final K key;

		public ValueBuilder(Type type, K key) {
			this.type = type;
			this.key = key;
		}

		public Builder<K> value(Object value) {
			return new Builder<>(type, key, value);
		}

	}

	public static class Builder<K> extends KeyValue.Builder<K, Object, Builder<K>> {

		private final Type type;

		public Builder(Type type, K key, Object value) {
			super(key, value);
			this.type = type;
		}

		@Override
		public DataStructure<K> build() {
			return new DataStructure<>(this);
		}
	}

}
