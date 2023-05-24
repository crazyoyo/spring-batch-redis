package com.redis.spring.batch.common;

import java.util.Arrays;

public class KeyDump<K> extends KeyTtlValue<K> {

	/**
	 * Redis value dump. Null if key does not exist
	 * 
	 */
	private byte[] dump;

	public byte[] getDump() {
		return dump;
	}

	public void setDump(byte[] dump) {
		this.dump = dump;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + Arrays.hashCode(dump);
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
		KeyDump<?> other = (KeyDump<?>) obj;
		return Arrays.equals(dump, other.dump);
	}

}
