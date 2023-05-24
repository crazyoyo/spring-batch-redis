package com.redis.spring.batch.reader;

public class ReadException extends Exception {

	private static final long serialVersionUID = 1L;

	public ReadException(Throwable cause) {
		super(cause);
	}

	public ReadException(String message) {
		super(message);
	}

	public ReadException(String message, Throwable cause) {
		super(message, cause);
	}

}
