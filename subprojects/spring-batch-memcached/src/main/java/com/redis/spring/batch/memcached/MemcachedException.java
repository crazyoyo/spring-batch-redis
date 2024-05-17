package com.redis.spring.batch.memcached;

@SuppressWarnings("serial")
public class MemcachedException extends RuntimeException {

	public MemcachedException(String msg) {
		super(msg);
	}

	public MemcachedException(String msg, Throwable cause) {
		super(msg, cause);
	}

	public MemcachedException(Throwable cause) {
		super(cause);
	}

}
