package com.redis.spring.batch.common;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class CompositeFuture<T> extends CompletableFuture<List<T>> implements Future<List<T>> {

	private final List<? extends Future<T>> futures;

	public CompositeFuture(List<? extends Future<T>> futures) {
		this.futures = futures;
	}

	@Override
	public boolean cancel(boolean mayInterruptIfRunning) {
		boolean anyCancelled = false;
		for (Future<T> future : futures) {
			anyCancelled |= future.cancel(mayInterruptIfRunning);
		}
		return anyCancelled;
	}

	@Override
	public boolean isCancelled() {
		for (Future<T> future : futures) {
			if (!future.isCancelled()) {
				return false;
			}
		}
		return true;
	}

	@Override
	public boolean isDone() {
		for (Future<T> future : futures) {
			if (!future.isDone()) {
				return false;
			}
		}
		return true;
	}

	@Override
	public List<T> get() throws InterruptedException, ExecutionException {
		List<T> results = new ArrayList<>();
		for (Future<T> future : futures) {
			results.add(future.get());
		}
		return results;
	}

	@Override
	public List<T> get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
		long doneTime = System.nanoTime() + unit.toNanos(timeout);
		List<T> results = new ArrayList<>();
		for (Future<T> future : futures) {
			long remaining = doneTime - System.nanoTime();
			results.add(future.get(remaining, TimeUnit.NANOSECONDS));
		}
		return results;
	}

}
