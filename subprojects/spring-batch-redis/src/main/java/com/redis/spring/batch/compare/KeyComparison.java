package com.redis.spring.batch.compare;

import com.redis.spring.batch.DataStructure;

public class KeyComparison {

	public enum Status {
		OK, // No difference
		MISSING, // Key missing in target database
		TYPE, // Type mismatch
		TTL, // TTL mismatch
		VALUE // Value mismatch
	}

	private DataStructure<String> source;
	private DataStructure<String> target;
	private Status status;

	public KeyComparison(DataStructure<String> source, DataStructure<String> target, Status status) {
		super();
		this.source = source;
		this.target = target;
		this.status = status;
	}

	public DataStructure<String> getSource() {
		return source;
	}

	public void setSource(DataStructure<String> source) {
		this.source = source;
	}

	public DataStructure<String> getTarget() {
		return target;
	}

	public void setTarget(DataStructure<String> target) {
		this.target = target;
	}

	public Status getStatus() {
		return status;
	}

	public void setStatus(Status status) {
		this.status = status;
	}

}
