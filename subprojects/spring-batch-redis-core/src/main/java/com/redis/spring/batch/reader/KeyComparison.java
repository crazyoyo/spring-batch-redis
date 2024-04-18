package com.redis.spring.batch.reader;

import com.redis.spring.batch.KeyValue;

public class KeyComparison {

	public enum Status {
		OK, // No difference
		MISSING, // Key missing in target database
		TYPE, // Type mismatch
		TTL, // TTL mismatch
		VALUE // Value mismatch
	}

	private KeyValue<String, Object> source;
	private KeyValue<String, Object> target;
	private Status status;

	public KeyValue<String, Object> getSource() {
		return source;
	}

	public void setSource(KeyValue<String, Object> source) {
		this.source = source;
	}

	public KeyValue<String, Object> getTarget() {
		return target;
	}

	public void setTarget(KeyValue<String, Object> target) {
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
