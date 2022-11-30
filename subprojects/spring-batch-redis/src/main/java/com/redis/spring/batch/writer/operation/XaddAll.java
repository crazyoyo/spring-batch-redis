package com.redis.spring.batch.writer.operation;

import java.util.ArrayList;
import java.util.Collection;
import java.util.function.Predicate;

import org.springframework.core.convert.converter.Converter;

import com.redis.spring.batch.writer.Operation;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.XAddArgs;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisStreamAsyncCommands;

public class XaddAll<K, V, T> extends AbstractCollectionAddAll<K, V, T> {

	private final Converter<T, Collection<StreamMessage<K, V>>> messages;
	private Converter<StreamMessage<K, V>, XAddArgs> args;

	public XaddAll(Predicate<T> delPredicate, Operation<K, V, T> del,
			Converter<T, Collection<StreamMessage<K, V>>> messages, Converter<StreamMessage<K, V>, XAddArgs> args) {
		super(delPredicate, del);
		this.messages = messages;
		this.args = args;
	}

	public void setArgs(Converter<StreamMessage<K, V>, XAddArgs> args) {
		this.args = args;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	protected Collection<RedisFuture> doExecute(BaseRedisAsyncCommands<K, V> commands, T item) {
		Collection<RedisFuture> futures = new ArrayList<>();
		for (StreamMessage<K, V> message : messages.convert(item)) {
			futures.add(((RedisStreamAsyncCommands<K, V>) commands).xadd(message.getStream(), args.convert(message),
					message.getBody()));
		}
		return futures;
	}

	public static <K, T> MessagesBuilder<K, T> key(Converter<T, K> key) {
		return new MessagesBuilder<>(key);
	}

	public static class MessagesBuilder<K, T> {

		private final Converter<T, K> key;

		public MessagesBuilder(Converter<T, K> key) {
			this.key = key;
		}

		public <V> Builder<K, V, T> messages(Converter<T, Collection<StreamMessage<K, V>>> messages) {
			return new Builder<>(key, messages);
		}
	}

	public static class Builder<K, V, T> extends DelBuilder<K, V, T, Builder<K, V, T>> {

		private final Converter<T, K> key;
		private final Converter<T, Collection<StreamMessage<K, V>>> messages;
		private Converter<StreamMessage<K, V>, XAddArgs> args = t -> null;

		public Builder(Converter<T, K> key, Converter<T, Collection<StreamMessage<K, V>>> messages) {
			this.key = key;
			this.messages = messages;
		}

		public Builder<K, V, T> args(XAddArgs args) {
			this.args = t -> args;
			return this;
		}

		public Builder<K, V, T> argsIdentity() {
			this.args = t -> new XAddArgs().id(t.getId());
			return this;
		}

		public XaddAll<K, V, T> build() {
			return new XaddAll<>(del, Del.of(key), messages, args);
		}

	}
}