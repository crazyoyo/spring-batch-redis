package com.redis.spring.batch.support;

public class OperationExecutionException extends Exception {

	private static final long serialVersionUID = 1L;

	public OperationExecutionException(String message, Throwable cause) {
		super(message, cause);
	}

	public OperationExecutionException(String message) {
		super(message);
	}

}
