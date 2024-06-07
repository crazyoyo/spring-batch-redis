package com.redis.spring.batch.item.redis.reader;

import java.time.Instant;

import com.redis.spring.batch.item.redis.common.DataType;

public class KeyNotification<K> {

	private K key;
	private String event;
	private Instant time;
	private DataType type;

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

	public Instant getTime() {
		return time;
	}

	public void setTime(Instant time) {
		this.time = time;
	}

	public DataType getType() {
		return type;
	}

	public void setType(DataType type) {
		this.type = type;
	}

	@Override
	public String toString() {
		return "KeyNotification [key=" + key + ", event=" + event + ", time=" + time + ", type=" + type + "]";
	}

}
