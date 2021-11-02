package com.redis.spring.batch.support.operation;

import java.util.Map;
import java.util.function.Predicate;

import org.springframework.core.convert.converter.Converter;
import org.springframework.util.Assert;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.XAddArgs;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisStreamAsyncCommands;
import lombok.Setter;
import lombok.experimental.Accessors;

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
		return ((RedisStreamAsyncCommands<K, V>) commands).xadd(key, args.convert(item), body.convert(item));
	}

	public static <T> XaddBodyBuilder<T> key(String key) {
		return key(t -> key);
	}

	public static <T> XaddBodyBuilder<T> key(Converter<T, String> key) {
		return new XaddBodyBuilder<>(key);
	}

	public static class XaddBodyBuilder<T> {

		private final Converter<T, String> key;

		public XaddBodyBuilder(Converter<T, String> key) {
			this.key = key;
		}

		public XaddBuilder<T> body(Converter<T, Map<String, String>> body) {
			return new XaddBuilder<>(key, body);
		}
	}

	@Setter
	@Accessors(fluent = true)
	public static class XaddBuilder<T> extends DelBuilder<T, XaddBuilder<T>> {

		private final Converter<T, String> key;
		private final Converter<T, Map<String, String>> body;
		private XAddArgs args;

		public XaddBuilder(Converter<T, String> key, Converter<T, Map<String, String>> body) {
			super(body);
			this.key = key;
			this.body = body;
		}

		@Override
		public Xadd<String, String, T> build() {
			return new Xadd<>(key, del, body, t -> args);
		}
	}

}
