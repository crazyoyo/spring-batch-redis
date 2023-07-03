package com.redis.spring.batch.common;

import java.util.Objects;

public class DataStructure<K> extends KeyValue<K> {

	/**
	 * Redis value. Null if key does not exist
	 */
	private Object value;

	@SuppressWarnings("unchecked")
	public <T> T getValue() {
		return (T) value;
	}

	public void setValue(Object value) {
		this.value = value;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + Objects.hash(value);
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
		@SuppressWarnings("rawtypes")
		DataStructure other = (DataStructure) obj;
		return Objects.equals(value, other.value);
	}

}
