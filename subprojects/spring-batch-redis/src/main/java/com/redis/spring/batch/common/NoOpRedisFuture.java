package com.redis.spring.batch.common;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import io.lettuce.core.RedisFuture;

public class NoOpRedisFuture<T> extends CompletableFuture<T> implements RedisFuture<T> {

	@SuppressWarnings("rawtypes")
	public static final NoOpRedisFuture NO_OP_REDIS_FUTURE = new NoOpRedisFuture();

	@Override
	public boolean cancel(boolean mayInterruptIfRunning) {
		return false;
	}

	@Override
	public boolean isCancelled() {
		return false;
	}

	@Override
	public boolean isDone() {
		return true;
	}

	@Override
	public T get() throws InterruptedException, ExecutionException {
		return null;
	}

	@Override
	public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
		return null;
	}

	@Override
	public String getError() {
		return null;
	}

	@Override
	public boolean await(long timeout, TimeUnit unit) throws InterruptedException {
		return false;
	}

}