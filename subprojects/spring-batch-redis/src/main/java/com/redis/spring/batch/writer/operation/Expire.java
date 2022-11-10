package com.redis.spring.batch.writer.operation;

import java.util.function.Predicate;

import org.springframework.core.convert.converter.Converter;
import org.springframework.util.Assert;

import com.redis.spring.batch.common.NoOpRedisFuture;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisKeyAsyncCommands;

public class Expire<K, V, T> extends AbstractKeyOperation<K, V, T> {

	private final Converter<T, Long> milliseconds;

	public Expire(Converter<T, K> key, Predicate<T> delete, Converter<T, Long> millis) {
		super(key, delete);
		Assert.notNull(millis, "A milliseconds converter is required");
		this.milliseconds = millis;
	}

	@SuppressWarnings("unchecked")
	@Override
	protected RedisFuture<Boolean> doExecute(BaseRedisAsyncCommands<K, V> commands, T item, K key) {
		Long millis = milliseconds.convert(item);
		if (millis == null) {
			return NoOpRedisFuture.NO_OP_REDIS_FUTURE;
		}
		if (millis < 0) {
			return NoOpRedisFuture.NO_OP_REDIS_FUTURE;
		}
		return ((RedisKeyAsyncCommands<K, V>) commands).pexpire(key, millis);
	}

	public static <K, T> MillisBuilder<K, T> key(Converter<T, K> key) {
		return new MillisBuilder<>(key);
	}

	public static class MillisBuilder<K, T> {

		private final Converter<T, K> key;

		public MillisBuilder(Converter<T, K> key) {
			this.key = key;
		}

		public <V> Builder<K, V, T> millis(Converter<T, Long> millis) {
			return new Builder<>(key, millis);
		}
	}

	public static class Builder<K, V, T> extends DelBuilder<K, V, T, Builder<K, V, T>> {

		private final Converter<T, K> key;
		private final Converter<T, Long> millis;

		public Builder(Converter<T, K> key, Converter<T, Long> millis) {
			this.key = key;
			this.millis = millis;
			onNull(millis);
		}

		public Expire<K, V, T> build() {
			return new Expire<>(key, del, millis);
		}

	}

}
