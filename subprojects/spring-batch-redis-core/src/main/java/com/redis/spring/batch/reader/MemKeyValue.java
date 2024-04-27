package com.redis.spring.batch.reader;

import java.util.Objects;

import com.redis.spring.batch.KeyValue;

public class MemKeyValue<K, T> extends KeyValue<K, T> {

	/**
	 * Number of bytes that this key and its value require to be stored in Redis
	 * RAM. 0 means no memory usage information is available.
	 */
	private long mem;

	public long getMem() {
		return mem;
	}

	public void setMem(long bytes) {
		this.mem = bytes;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + Objects.hash(mem);
		return result;
	}

	@SuppressWarnings("rawtypes")
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (getClass() != obj.getClass())
			return false;
		MemKeyValue other = (MemKeyValue) obj;
		return mem == other.mem;
	}

}
