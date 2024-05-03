package com.redis.spring.batch.reader;

import com.redis.spring.batch.common.KeyValue;

public class KeyComparison<K> {

	public enum Status {
		OK, // No difference
		MISSING, // Key missing in target database
		TYPE, // Type mismatch
		TTL, // TTL mismatch
		VALUE // Value mismatch
	}

	private KeyValue<K, Object> source;
	private KeyValue<K, Object> target;
	private Status status;

	public KeyValue<K, Object> getSource() {
		return source;
	}

	public void setSource(KeyValue<K, Object> source) {
		this.source = source;
	}

	public KeyValue<K, Object> getTarget() {
		return target;
	}

	public void setTarget(KeyValue<K, Object> target) {
		this.target = target;
	}

	public Status getStatus() {
		return status;
	}

	public void setStatus(Status status) {
		this.status = status;
	}

	@Override
	public String toString() {
		return "KeyComparison [source=" + source + ", target=" + target + ", status=" + status + "]";
	}

}
