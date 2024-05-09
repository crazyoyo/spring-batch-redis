package com.redis.spring.batch.memcached.common;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import net.spy.memcached.internal.OperationFuture;

public abstract class MemcachedUtils {

	private MemcachedUtils() {
	}

	public static <T> List<T> get(Duration timeout, OperationFuture<T> future)
			throws TimeoutException, InterruptedException, ExecutionException {
		return getAll(timeout, Arrays.asList(future));
	}

	@SuppressWarnings("unchecked")
	public static <T> List<T> getAll(Duration timeout, OperationFuture<T>... futures)
			throws TimeoutException, InterruptedException, ExecutionException {
		return getAll(timeout, Arrays.asList(futures));
	}

	public static <T> List<T> getAll(Duration timeout, Iterable<OperationFuture<T>> futures)
			throws TimeoutException, InterruptedException, ExecutionException {
		List<T> items = new ArrayList<>();
		long nanos = timeout.toNanos();
		long time = System.nanoTime();
		for (OperationFuture<T> f : futures) {
			if (timeout.isNegative()) {
				items.add(f.get());
			} else {
				if (nanos < 0) {
					throw new TimeoutException(String.format("Timed out after %s", timeout));
				}
				T item = f.get(nanos, TimeUnit.NANOSECONDS);
				items.add(item);
				long now = System.nanoTime();
				nanos -= now - time;
				time = now;
			}
		}
		return items;
	}
}