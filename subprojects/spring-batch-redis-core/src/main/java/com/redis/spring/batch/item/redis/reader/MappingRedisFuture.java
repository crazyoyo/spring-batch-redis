package com.redis.spring.batch.item.redis.reader;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import io.lettuce.core.RedisFuture;

public class MappingRedisFuture<T, S> extends CompletableFuture<T> implements RedisFuture<T> {

	private final CountDownLatch latch = new CountDownLatch(1);

	public MappingRedisFuture(CompletionStage<S> completionStage, Function<S, T> converter) {
		completionStage.thenAccept(v -> complete(converter.apply(v))).exceptionally(throwable -> {
			completeExceptionally(throwable);
			return null;
		});
	}

	@Override
	public boolean complete(T value) {
		boolean result = super.complete(value);
		latch.countDown();
		return result;
	}

	@Override
	public boolean completeExceptionally(Throwable ex) {
		boolean value = super.completeExceptionally(ex);
		latch.countDown();
		return value;
	}

	@Override
	public String getError() {
		return null;
	}

	@Override
	public boolean await(long timeout, TimeUnit unit) throws InterruptedException {
		return latch.await(timeout, unit);
	}

}
