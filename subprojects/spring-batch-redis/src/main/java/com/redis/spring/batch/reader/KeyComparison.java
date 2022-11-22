package com.redis.spring.batch.reader;

import java.util.Objects;

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

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + Objects.hash(source, status, target);
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (getClass() != obj.getClass())
			return false;
		KeyComparison<?> other = (KeyComparison<?>) obj;
		return Objects.equals(source, other.source) && status == other.status && Objects.equals(target, other.target);
	}

}
