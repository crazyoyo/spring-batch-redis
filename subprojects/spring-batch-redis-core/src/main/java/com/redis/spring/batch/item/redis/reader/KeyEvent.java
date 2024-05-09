package com.redis.spring.batch.item.redis.reader;

import java.util.Objects;

public class KeyEvent<K> {

	private final K key;
	private final String event;

	public KeyEvent(K key, String event) {
		this.key = key;
		this.event = event;
	}

	public K getKey() {
		return key;
	}

	public String getEvent() {
		return event;
	}

	@Override
	public int hashCode() {
		return Objects.hash(key);
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
		KeyEvent other = (KeyEvent) obj;
		return Objects.equals(key, other.key);
	}

}
