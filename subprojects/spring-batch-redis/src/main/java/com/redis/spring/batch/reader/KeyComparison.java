package com.redis.spring.batch.reader;

import java.util.Objects;

import com.redis.spring.batch.common.KeyValue;

public class KeyComparison {

	public enum Status {
		OK, // No difference
		MISSING, // Key missing in target database
		TYPE, // Type mismatch
		TTL, // TTL mismatch
		VALUE // Value mismatch
	}

	private KeyValue<String> source;
	private KeyValue<String> target;
	private Status status;

	public KeyComparison(KeyValue<String> source, KeyValue<String> target, Status status) {
		this.source = source;
		this.target = target;
		this.status = status;
	}

	public KeyValue<String> getSource() {
		return source;
	}

	public void setSource(KeyValue<String> source) {
		this.source = source;
	}

	public KeyValue<String> getTarget() {
		return target;
	}

	public void setTarget(KeyValue<String> target) {
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
		KeyComparison other = (KeyComparison) obj;
		return Objects.equals(source, other.source) && status == other.status && Objects.equals(target, other.target);
	}

}
