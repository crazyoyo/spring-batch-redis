package com.redis.spring.batch.operation;

import java.util.Collection;
import java.util.List;
import java.util.function.Function;

import org.springframework.util.CollectionUtils;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.XAddArgs;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisStreamAsyncCommands;

public class XaddAll<K, V, T> implements Operation<K, V, T, Object> {

	private final Function<T, Collection<StreamMessage<K, V>>> messagesFunction;
	private Function<StreamMessage<K, V>, XAddArgs> argsFunction = m -> new XAddArgs().id(m.getId());

	public XaddAll(Function<T, Collection<StreamMessage<K, V>>> messagesFunction) {
		this.messagesFunction = messagesFunction;
	}

	public void setArgs(XAddArgs args) {
		this.argsFunction = t -> args;
	}

	public void setArgsFunction(Function<StreamMessage<K, V>, XAddArgs> function) {
		this.argsFunction = function;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public void execute(BaseRedisAsyncCommands<K, V> commands, Iterable<? extends T> items,
			List<RedisFuture<Object>> futures) {
		RedisStreamAsyncCommands<K, V> streamCommands = (RedisStreamAsyncCommands<K, V>) commands;
		for (T item : items) {
			Collection<StreamMessage<K, V>> messages = messagesFunction.apply(item);
			if (CollectionUtils.isEmpty(messages)) {
				continue;
			}
			for (StreamMessage<K, V> message : messages) {
				XAddArgs args = argsFunction.apply(message);
				futures.add((RedisFuture) streamCommands.xadd(message.getStream(), args, message.getBody()));
			}
		}
	}

}
