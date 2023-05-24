package com.redis.spring.batch.common;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

import io.lettuce.core.RedisFuture;

public class ConvertingRedisFuture<I, O> implements RedisFuture<O> {

	private final RedisFuture<I> delegate;
	private final Function<I, O> mapper;

	public ConvertingRedisFuture(RedisFuture<I> delegate, Function<I, O> mapper) {
		this.delegate = delegate;
		this.mapper = mapper;
	}

	@Override
	public boolean cancel(boolean mayInterruptIfRunning) {
		return delegate.cancel(mayInterruptIfRunning);
	}

	@Override
	public boolean isCancelled() {
		return delegate.isCancelled();
	}

	@Override
	public boolean isDone() {
		return delegate.isDone();
	}

	@Override
	public O get() throws InterruptedException, ExecutionException {
		return mapper.apply(delegate.get());
	}

	@Override
	public O get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
		return mapper.apply(delegate.get(timeout, unit));
	}

	@Override
	public <U> CompletionStage<U> thenApply(Function<? super O, ? extends U> fn) {
		return delegate.thenApply(mapper.andThen(fn));
	}

	@Override
	public <U> CompletionStage<U> thenApplyAsync(Function<? super O, ? extends U> fn) {
		return delegate.thenApplyAsync(mapper.andThen(fn));
	}

	@Override
	public <U> CompletionStage<U> thenApplyAsync(Function<? super O, ? extends U> fn, Executor executor) {
		return delegate.thenApplyAsync(mapper.andThen(fn), executor);
	}

	@Override
	public CompletionStage<Void> thenAccept(Consumer<? super O> action) {
		return chain().thenAccept(action);
	}

	@Override
	public CompletionStage<Void> thenAcceptAsync(Consumer<? super O> action) {
		return chain().thenAcceptAsync(action);
	}

	@Override
	public CompletionStage<Void> thenAcceptAsync(Consumer<? super O> action, Executor executor) {
		return chain().thenAcceptAsync(action, executor);
	}

	@Override
	public CompletionStage<Void> thenRun(Runnable action) {
		return chain().thenRun(action);
	}

	@Override
	public CompletionStage<Void> thenRunAsync(Runnable action) {
		return chain().thenRunAsync(action);
	}

	@Override
	public CompletionStage<Void> thenRunAsync(Runnable action, Executor executor) {
		return chain().thenRunAsync(action, executor);
	}

	@Override
	public <U, V> CompletionStage<V> thenCombine(CompletionStage<? extends U> other,
			BiFunction<? super O, ? super U, ? extends V> fn) {
		return chain().thenCombine(other, fn);
	}

	@Override
	public <U, V> CompletionStage<V> thenCombineAsync(CompletionStage<? extends U> other,
			BiFunction<? super O, ? super U, ? extends V> fn) {
		return chain().thenCombineAsync(other, fn);
	}

	@Override
	public <U, V> CompletionStage<V> thenCombineAsync(CompletionStage<? extends U> other,
			BiFunction<? super O, ? super U, ? extends V> fn, Executor executor) {
		return chain().thenCombineAsync(other, fn, executor);
	}

	@Override
	public <U> CompletionStage<Void> thenAcceptBoth(CompletionStage<? extends U> other,
			BiConsumer<? super O, ? super U> action) {
		return chain().thenAcceptBoth(other, action);
	}

	@Override
	public <U> CompletionStage<Void> thenAcceptBothAsync(CompletionStage<? extends U> other,
			BiConsumer<? super O, ? super U> action) {
		return chain().thenAcceptBothAsync(other, action);
	}

	@Override
	public <U> CompletionStage<Void> thenAcceptBothAsync(CompletionStage<? extends U> other,
			BiConsumer<? super O, ? super U> action, Executor executor) {
		return chain().thenAcceptBothAsync(other, action, executor);
	}

	@Override
	public CompletionStage<Void> runAfterBoth(CompletionStage<?> other, Runnable action) {
		return chain().runAfterBoth(other, action);
	}

	@Override
	public CompletionStage<Void> runAfterBothAsync(CompletionStage<?> other, Runnable action) {
		return chain().runAfterBothAsync(other, action);
	}

	@Override
	public CompletionStage<Void> runAfterBothAsync(CompletionStage<?> other, Runnable action, Executor executor) {
		return chain().runAfterBothAsync(other, action, executor);
	}

	@Override
	public <U> CompletionStage<U> applyToEither(CompletionStage<? extends O> other, Function<? super O, U> fn) {
		return chain().applyToEither(other, fn);
	}

	@Override
	public <U> CompletionStage<U> applyToEitherAsync(CompletionStage<? extends O> other, Function<? super O, U> fn) {
		return chain().applyToEitherAsync(other, fn);
	}

	@Override
	public <U> CompletionStage<U> applyToEitherAsync(CompletionStage<? extends O> other, Function<? super O, U> fn,
			Executor executor) {
		return chain().applyToEitherAsync(other, fn, executor);
	}

	@Override
	public CompletionStage<Void> acceptEither(CompletionStage<? extends O> other, Consumer<? super O> action) {
		return chain().acceptEither(other, action);
	}

	@Override
	public CompletionStage<Void> acceptEitherAsync(CompletionStage<? extends O> other, Consumer<? super O> action) {
		return chain().acceptEitherAsync(other, action);
	}

	@Override
	public CompletionStage<Void> acceptEitherAsync(CompletionStage<? extends O> other, Consumer<? super O> action,
			Executor executor) {
		return chain().acceptEitherAsync(other, action, executor);
	}

	@Override
	public CompletionStage<Void> runAfterEither(CompletionStage<?> other, Runnable action) {
		return chain().runAfterEither(other, action);
	}

	@Override
	public CompletionStage<Void> runAfterEitherAsync(CompletionStage<?> other, Runnable action) {
		return chain().runAfterEitherAsync(other, action);
	}

	@Override
	public CompletionStage<Void> runAfterEitherAsync(CompletionStage<?> other, Runnable action, Executor executor) {
		return chain().runAfterEitherAsync(other, action, executor);
	}

	@Override
	public <U> CompletionStage<U> thenCompose(Function<? super O, ? extends CompletionStage<U>> fn) {
		return chain().thenCompose(fn);
	}

	@Override
	public <U> CompletionStage<U> thenComposeAsync(Function<? super O, ? extends CompletionStage<U>> fn) {
		return chain().thenComposeAsync(fn);
	}

	@Override
	public <U> CompletionStage<U> thenComposeAsync(Function<? super O, ? extends CompletionStage<U>> fn,
			Executor executor) {
		return chain().thenComposeAsync(fn, executor);
	}

	@Override
	public <U> CompletionStage<U> handle(BiFunction<? super O, Throwable, ? extends U> fn) {
		return chain().handle(fn);
	}

	@Override
	public <U> CompletionStage<U> handleAsync(BiFunction<? super O, Throwable, ? extends U> fn) {
		return chain().handleAsync(fn);
	}

	@Override
	public <U> CompletionStage<U> handleAsync(BiFunction<? super O, Throwable, ? extends U> fn, Executor executor) {
		return chain().handleAsync(fn, executor);
	}

	@Override
	public CompletionStage<O> whenComplete(BiConsumer<? super O, ? super Throwable> action) {
		return chain().whenComplete(action);
	}

	@Override
	public CompletionStage<O> whenCompleteAsync(BiConsumer<? super O, ? super Throwable> action) {
		return chain().whenCompleteAsync(action);
	}

	@Override
	public CompletionStage<O> whenCompleteAsync(BiConsumer<? super O, ? super Throwable> action, Executor executor) {
		return chain().whenCompleteAsync(action, executor);
	}

	@Override
	public CompletionStage<O> exceptionally(Function<Throwable, ? extends O> fn) {
		return chain().exceptionally(fn);
	}

	private CompletionStage<O> chain() {
		return chain();
	}

	@Override
	public CompletableFuture<O> toCompletableFuture() {
		return chain().toCompletableFuture();
	}

	@Override
	public String getError() {
		return delegate.getError();
	}

	@Override
	public boolean await(long timeout, TimeUnit unit) throws InterruptedException {
		return delegate.await(timeout, unit);
	}
}
