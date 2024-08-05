package com.redis.spring.batch.item.redis.common;

import java.util.Arrays;

public class KeyEvent<K> {

	private K key;
	private String event;
	private long timestamp;

	public K getKey() {
		return key;
	}

	public void setKey(K key) {
		this.key = key;
	}

	/**
	 * 
	 * @return POSIX time in milliseconds when the event happened
	 */
	public long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(long time) {
		this.timestamp = time;
	}

	/**
	 * 
	 * @return the code that originated this event (e.g. scan, del, ...)
	 */
	public String getEvent() {
		return event;
	}

	public void setEvent(String event) {
		this.event = event;
	}

	@Override
	public String toString() {
		return "KeyEvent [key=" + key + "]";
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		KeyEvent<K> other = (KeyEvent<K>) obj;
		return keyEquals(this.key, other.key);
	}

	public static <K> boolean keyEquals(K key, K other) {
		if (key == other) {
			return true;
		}
		if (key == null) {
			return false;
		}
		if (key instanceof byte[] && other instanceof byte[]) {
			return Arrays.equals((byte[]) key, (byte[]) other);
		}
		return key.equals(other);
	}

	public static boolean hasKey(KeyEvent<?> keyEvent) {
		return keyEvent.key != null;
	}

	public static <K> KeyEvent<K> of(K key, String event) {
		KeyEvent<K> keyEvent = new KeyEvent<>();
		keyEvent.setKey(key);
		keyEvent.setEvent(event);
		keyEvent.setTimestamp(System.currentTimeMillis());
		return keyEvent;
	}

}
