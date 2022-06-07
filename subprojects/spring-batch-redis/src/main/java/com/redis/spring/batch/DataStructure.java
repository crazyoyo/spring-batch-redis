package com.redis.spring.batch;

import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.springframework.util.Assert;

public class DataStructure<K> extends KeyValue<K, Object> {

	private String type;

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public void setType(Type type) {
		Assert.notNull(type, "Type must not be null");
		this.type = type.getString();
	}

	@Override
	public String toString() {
		return "DataStructure [type=" + type + ", key=" + getKey() + ", value=" + getValue() + ", absoluteTTL="
				+ getTtl() + "]";
	}

	public enum Type {

		SET, LIST, ZSET, STREAM, STRING, HASH, NONE, JSON("ReJSON-RL"), TIMESERIES("TSDB-TYPE");

		private static final Map<String, Type> TYPES = Stream.of(Type.values())
				.collect(Collectors.toMap(Type::getString, t -> t));

		private String name;

		Type() {
			this.name = name().toLowerCase();
		}

		Type(String name) {
			this.name = name;
		}

		/**
		 * 
		 * @return The Redis type string
		 */
		public String getString() {
			return name;
		}

		@Override
		public String toString() {
			return name;
		}

		public static Type of(String name) {
			if (name == null) {
				throw new NullPointerException("Name is null");
			}
			Type result = TYPES.get(name);
			if (result != null) {
				return result;
			}
			throw new IllegalArgumentException("No type with name " + name);
		}

	}

}
