package com.redis.spring.batch.item.redis.reader;

public class KeyNotificationStatusCount {

	private final KeyNotificationStatus status;
	private final long count;

	public KeyNotificationStatusCount(KeyNotificationStatus status, long count) {
		this.status = status;
		this.count = count;
	}

	public KeyNotificationStatus getStatus() {
		return status;
	}

	public long getCount() {
		return count;
	}

}
