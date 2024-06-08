package com.redis.spring.batch.item.redis.reader;

public class KeyNotification<K> {

	private K key;
	private String event;
	private long time;
	private String type;

	public K getKey() {
		return key;
	}

	public void setKey(K key) {
		this.key = key;
	}

	public String getEvent() {
		return event;
	}

	public void setEvent(String event) {
		this.event = event;
	}

	public long getTime() {
		return time;
	}

	public void setTime(long time) {
		this.time = time;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	@Override
	public String toString() {
		return "KeyNotification [key=" + key + ", event=" + event + ", time=" + time + ", type=" + type + "]";
	}

}
