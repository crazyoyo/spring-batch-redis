package com.redis.spring.batch.item.redis.common;

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
