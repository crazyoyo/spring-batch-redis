package com.redis.spring.batch.reader;

import java.util.Objects;

import com.redis.spring.batch.common.DataStructure;
import com.redis.spring.batch.common.KeyValue;

public class KeyComparison extends KeyValue<String> {

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

	@Override
	public String getKey() {
		return source.getKey();
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
