package com.redis.spring.batch.item.redis.reader;

public class KeyNotificationStatusCount {

	private final KeyEventStatus status;
	private final long count;

	public KeyNotificationStatusCount(KeyEventStatus status, long count) {
		this.status = status;
		this.count = count;
	}

	public KeyEventStatus getStatus() {
		return status;
	}

	public long getCount() {
		return count;
	}

}
