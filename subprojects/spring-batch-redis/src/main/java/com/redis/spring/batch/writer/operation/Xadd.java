package com.redis.spring.batch.writer.operation;

import java.util.Map;
import java.util.function.Predicate;

import org.springframework.core.convert.converter.Converter;
import org.springframework.util.Assert;

import com.redis.spring.batch.common.NoOpRedisFuture;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.XAddArgs;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisStreamAsyncCommands;

public class Xadd<K, V, T> extends AbstractKeyOperation<K, V, T> {

	private final Converter<T, XAddArgs> args;
	private final Converter<T, Map<K, V>> body;

	public Xadd(Converter<T, K> key, Predicate<T> delete, Converter<T, Map<K, V>> body, Converter<T, XAddArgs> args) {
		super(key, delete);
		Assert.notNull(body, "A body converter is required");
		Assert.notNull(args, "A XAddArgs converter is required");
		this.body = body;
		this.args = args;
	}

	@SuppressWarnings("unchecked")
	@Override
	protected RedisFuture<String> doExecute(BaseRedisAsyncCommands<K, V> commands, T item, K key) {
		Map<K, V> map = body.convert(item);
		if (map == null || map.isEmpty()) {
			return NoOpRedisFuture.NO_OP_REDIS_FUTURE;
		}
		return ((RedisStreamAsyncCommands<K, V>) commands).xadd(key, args.convert(item), map);
	}

	public static <K, V> Converter<StreamMessage<K, V>, XAddArgs> identity() {
		return m -> new XAddArgs().id(m.getId());
	}

	public static <K, T> BodyBuilder<K, T> key(K key) {
		return key(t -> key);
	}

	public static <K, T> BodyBuilder<K, T> key(Converter<T, K> key) {
		return new BodyBuilder<>(key);
	}

	public static class BodyBuilder<K, T> {

		private final Converter<T, K> key;

		public BodyBuilder(Converter<T, K> key) {
			this.key = key;
		}

		public <V> Builder<K, V, T> body(Converter<T, Map<K, V>> body) {
			return new Builder<>(key, body);
		}
	}

	public static class Builder<K, V, T> extends DelBuilder<K, V, T, Builder<K, V, T>> {

		private final Converter<T, K> key;
		private final Converter<T, Map<K, V>> body;
		private Converter<T, XAddArgs> args = t -> null;

		public Builder(Converter<T, K> key, Converter<T, Map<K, V>> body) {
			this.key = key;
			this.body = body;
			onNull(body);
		}

		public Builder<K, V, T> args(XAddArgs args) {
			this.args = t -> args;
			return this;
		}

		public Xadd<K, V, T> build() {
			return new Xadd<>(key, del, body, args);
		}

	}

}
