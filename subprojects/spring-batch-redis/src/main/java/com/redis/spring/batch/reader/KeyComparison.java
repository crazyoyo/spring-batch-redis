package com.redis.spring.batch.reader;

import com.redis.spring.batch.common.DataStructure;
import com.redis.spring.batch.common.KeyValue;

public class KeyComparison<K> extends KeyValue<K> {

	public enum Status {
		OK, // No difference
		MISSING, // Key missing in target database
		TYPE, // Type mismatch
		TTL, // TTL mismatch
		VALUE // Value mismatch
	}

	private DataStructure<K> source;
	private DataStructure<K> target;
	private Status status;

	public KeyComparison(DataStructure<K> source, DataStructure<K> target, Status status) {
		super();
		this.source = source;
		this.target = target;
		this.status = status;
	}

	@Override
	public K getKey() {
		return source.getKey();
	}

	public DataStructure<K> getSource() {
		return source;
	}

	public void setSource(DataStructure<K> source) {
		this.source = source;
	}

	public DataStructure<K> getTarget() {
		return target;
	}

	public void setTarget(DataStructure<K> target) {
		this.target = target;
	}

	public Status getStatus() {
		return status;
	}

	public void setStatus(Status status) {
		this.status = status;
	}

}
