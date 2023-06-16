package com.redis.spring.batch.reader;

public class PollingException extends Exception {

	public PollingException(Exception e) {
		super(e);
	}

	private static final long serialVersionUID = 1L;

}