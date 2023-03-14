package com.redis.spring.batch.reader;

public class KeyspaceNotification {

	private final String key;
	private final KeyEventType eventType;

	public KeyspaceNotification(String key, KeyEventType eventType) {
		this.key = key;
		this.eventType = eventType;
	}

	public String getKey() {
		return key;
	}

	public KeyEventType getEventType() {
		return eventType;
	}

	@Override
	public int hashCode() {
		return key.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof KeyspaceNotification)) {
			return false;
		}
		KeyspaceNotification that = (KeyspaceNotification) obj;
		return key.equals(that.key);
	}

}