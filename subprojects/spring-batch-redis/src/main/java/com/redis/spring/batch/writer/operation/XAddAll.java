package com.redis.spring.batch.writer.operation;

import java.util.Collection;
import java.util.List;
import java.util.function.Function;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.XAddArgs;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisStreamAsyncCommands;

public class XAddAll<K, V, T> extends AbstractAddAllOperation<K, V, T, StreamMessage<K, V>> {

	private final Function<StreamMessage<K, V>, XAddArgs> args;

	public XAddAll(Function<T, K> key, Function<T, Collection<StreamMessage<K, V>>> messages) {
		this(key, messages, t -> null);
	}

	public XAddAll(Function<T, K> key, Function<T, Collection<StreamMessage<K, V>>> messages,
			Function<StreamMessage<K, V>, XAddArgs> args) {
		super(key, messages);
		this.args = args;
	}

	@Override
	@SuppressWarnings("unchecked")
	protected void execute(BaseRedisAsyncCommands<K, V> commands, List<RedisFuture<?>> futures, T item, K key,
			Collection<StreamMessage<K, V>> messages) {
		RedisStreamAsyncCommands<K, V> streamCommands = (RedisStreamAsyncCommands<K, V>) commands;
		for (StreamMessage<K, V> message : messages) {
			futures.add(streamCommands.xadd(key, args.apply(message), message.getBody()));
		}
	}

}