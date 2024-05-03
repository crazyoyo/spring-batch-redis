package com.redis.spring.batch.reader;

import com.redis.spring.batch.common.KeyValue;

public class MemKeyValue<K, T> extends KeyValue<K, T> {

	private long mem;

	public MemKeyValue() {
	}

	public MemKeyValue(KeyValue<K, T> other) {
		super(other);
	}

	public MemKeyValue(MemKeyValue<K, T> other) {
		super(other);
		this.mem = other.mem;
	}

	/**
	 * 
	 * @return Number of bytes that this key and its value require to be stored in
	 *         Redis RAM. 0 means no memory usage information is available.
	 */
	public long getMem() {
		return mem;
	}

	public void setMem(long bytes) {
		this.mem = bytes;
	}

}
