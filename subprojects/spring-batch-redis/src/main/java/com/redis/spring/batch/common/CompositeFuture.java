package com.redis.spring.batch.common;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import io.lettuce.core.RedisFuture;

public class CompositeFuture<T> extends CompletableFuture<List<T>> implements RedisFuture<List<T>> {

	private final List<? extends RedisFuture<T>> futures;

	public CompositeFuture(List<? extends RedisFuture<T>> futures) {
		this.futures = futures;
	}

	@Override
	public boolean cancel(boolean mayInterruptIfRunning) {
		boolean anyCancelled = false;
		for (RedisFuture<T> future : futures) {
			anyCancelled |= future.cancel(mayInterruptIfRunning);
		}
		return anyCancelled;
	}

	@Override
	public boolean isCancelled() {
		for (RedisFuture<T> future : futures) {
			if (!future.isCancelled()) {
				return false;
			}
		}
		return true;
	}

	@Override
	public boolean isDone() {
		for (RedisFuture<T> future : futures) {
			if (!future.isDone()) {
				return false;
			}
		}
		return true;
	}

	@Override
	public List<T> get() throws InterruptedException, ExecutionException {
		List<T> results = new ArrayList<>();
		for (RedisFuture<T> future : futures) {
			results.add(future.get());
		}
		return results;
	}

	@Override
	public List<T> get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
		long doneTime = System.nanoTime() + unit.toNanos(timeout);
		List<T> results = new ArrayList<>();
		for (RedisFuture<T> future : futures) {
			long remaining = doneTime - System.nanoTime();
			results.add(future.get(remaining, TimeUnit.NANOSECONDS));
		}
		return results;
	}

	@Override
	public String getError() {
		return futures.stream().map(RedisFuture::getError).filter(Objects::nonNull).collect(Collectors.joining(","));
	}

	@Override
	public boolean await(long timeout, TimeUnit unit) throws InterruptedException {
		long doneTime = System.nanoTime() + unit.toNanos(timeout);
		for (RedisFuture<T> future : futures) {
			long remaining = doneTime - System.nanoTime();
			if (future.await(remaining, TimeUnit.NANOSECONDS)) {
				return false;
			}
		}
		return true;
	}

}
