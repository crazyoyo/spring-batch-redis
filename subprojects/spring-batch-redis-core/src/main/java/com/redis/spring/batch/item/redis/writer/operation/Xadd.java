package com.redis.spring.batch.item.redis.writer.operation;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.springframework.util.CollectionUtils;

import com.redis.spring.batch.item.redis.common.BatchUtils;
import com.redis.spring.batch.item.redis.common.Operation;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.XAddArgs;
import io.lettuce.core.api.async.RedisAsyncCommands;

public class Xadd<K, V, T> implements Operation<K, V, T, Object> {

	private final Function<T, Collection<StreamMessage<K, V>>> messagesFunction;

	private Function<StreamMessage<K, V>, XAddArgs> argsFunction = this::defaultArgs;

	public Xadd(Function<T, Collection<StreamMessage<K, V>>> messagesFunction) {
		this.messagesFunction = messagesFunction;
	}

	private XAddArgs defaultArgs(StreamMessage<K, V> message) {
		if (message == null || message.getId() == null) {
			return null;
		}
		return new XAddArgs().id(message.getId());
	}

	public void setArgs(XAddArgs args) {
		setArgsFunction(t -> args);
	}

	public void setArgsFunction(Function<StreamMessage<K, V>, XAddArgs> function) {
		this.argsFunction = function;
	}

	private Stream<StreamMessage<K, V>> messages(T item) {
		return messagesFunction.apply(item).stream();
	}

	@Override
	public List<RedisFuture<Object>> execute(RedisAsyncCommands<K, V> commands, Iterable<? extends T> items) {
		return BatchUtils.stream(items).flatMap(this::messages).map(m -> execute(commands, m))
				.collect(Collectors.toList());
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private RedisFuture<Object> execute(RedisAsyncCommands<K, V> commands, StreamMessage<K, V> message) {
		Map<K, V> body = message.getBody();
		if (CollectionUtils.isEmpty(body)) {
			return null;
		}
		K stream = message.getStream();
		XAddArgs args = argsFunction.apply(message);
		return (RedisFuture) commands.xadd(stream, args, body);
	}

}
