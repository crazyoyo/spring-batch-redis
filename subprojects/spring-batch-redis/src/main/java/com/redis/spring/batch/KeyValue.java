package com.redis.spring.batch;

import java.util.Objects;

public class KeyValue<K, T> {

	/**
	 * Redis key.
	 *
	 */
	private K key;

	/**
	 * Redis value. Null if key does not exist
	 * 
	 */
	private T value;

	/**
	 * Expiration POSIX time in milliseconds for this key.
	 *
	 */
	private Long ttl;

	public KeyValue() {
	}

	protected KeyValue(Builder<K, T, ?> builder) {
		this.key = builder.key;
		this.value = builder.value;
		this.ttl = builder.ttl;
	}

	public boolean hasTtl() {
		return ttl != null && ttl > 0;
	}

	public K getKey() {
		return key;
	}

	public void setKey(K key) {
		this.key = key;
	}

	public T getValue() {
		return value;
	}

	public void setValue(T value) {
		this.value = value;
	}

	public Long getTtl() {
		return ttl;
	}

	public void setTtl(long absoluteTtl) {
		this.ttl = absoluteTtl;
	}

	@Override
	public String toString() {
		return "KeyValue [key=" + key + ", value=" + value + ", ttl=" + ttl + "]";
	}

	@Override
	public int hashCode() {
		return Objects.hash(key, ttl, value);
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
		return Objects.equals(key, other.key) && Objects.equals(ttl, other.ttl) && Objects.equals(value, other.value);
	}

	protected abstract static class Builder<K, T, B extends Builder<K, T, B>> {

		private final K key;
		private final T value;
		private Long ttl;

		protected Builder(K key, T value) {
			this.key = key;
			this.value = value;
		}

		@SuppressWarnings("unchecked")
		public B ttl(Long ttl) {
			this.ttl = ttl;
			return (B) this;
		}

		public KeyValue<K, T> build() {
			return new KeyValue<>(this);
		}

	}

}
