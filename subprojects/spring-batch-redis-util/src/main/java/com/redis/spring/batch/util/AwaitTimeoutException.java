package com.redis.spring.batch.util;

/**
 * A runtime exception thrown by Await when a condition was not fulfilled within
 * the specified threshold.
 *
 */
public class AwaitTimeoutException extends RuntimeException {

	private static final long serialVersionUID = 1L;

	/**
	 * <p>
	 * Constructor for AwaitTimeoutException.
	 * </p>
	 *
	 * @param message A description of why the timeout occurred.
	 */
	public AwaitTimeoutException(String message) {
		super(message);
	}

	/**
	 * <p>
	 * Constructor for AwaitTimeoutException.
	 * </p>
	 *
	 * @param message   A description of why the timeout occurred.
	 * @param throwable The cause
	 */
	public AwaitTimeoutException(String message, Throwable throwable) {
		super(message, throwable);
	}
}
