package com.redis.spring.batch.gen;

import java.util.Objects;

public class Item {

	public enum Type {
		HASH, JSON, LIST, SET, STREAM, STRING, TIMESERIES, ZSET
	}

	private String key;
	private Type type;
	private Object value;
	private long ttl;

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
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

	@Override
	public int hashCode() {
		return Objects.hash(key, ttl, type, value);
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Item other = (Item) obj;
		return Objects.equals(key, other.key) && ttl == other.ttl && type == other.type
				&& Objects.equals(value, other.value);
	}

}
