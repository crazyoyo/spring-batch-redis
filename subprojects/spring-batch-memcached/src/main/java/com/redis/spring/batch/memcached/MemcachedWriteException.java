package com.redis.spring.batch.memcached;

@SuppressWarnings("serial")
public class MemcachedWriteException extends Exception {

	public MemcachedWriteException(String message, Throwable cause) {
		super(message, cause);
	}

	public MemcachedWriteException(String message) {
		super(message);
	}

	public MemcachedWriteException(Throwable cause) {
		super(cause);
	}

}
