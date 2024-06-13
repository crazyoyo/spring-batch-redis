package com.redis.spring.batch.item.redis.writer.operation;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.springframework.util.CollectionUtils;

import com.redis.spring.batch.item.redis.common.BatchUtils;
import com.redis.spring.batch.item.redis.writer.AbstractValueWriteOperation;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.XAddArgs;
import io.lettuce.core.api.async.RedisAsyncCommands;

public class Xadd<K, V, T> extends AbstractValueWriteOperation<K, V, Collection<StreamMessage<K, V>>, T> {

	private Function<StreamMessage<K, V>, XAddArgs> argsFunction = this::defaultArgs;

	public Xadd(Function<T, K> keyFunction, Function<T, Collection<StreamMessage<K, V>>> messagesFunction) {
		super(keyFunction, messagesFunction);
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

	@Override
	public List<RedisFuture<Object>> execute(RedisAsyncCommands<K, V> commands, Iterable<? extends T> items) {
		return BatchUtils.stream(items).flatMap(t -> execute(commands, t)).collect(Collectors.toList());
	}

	private Stream<RedisFuture<Object>> execute(RedisAsyncCommands<K, V> commands, T item) {
		K key = key(item);
		Collection<StreamMessage<K, V>> messages = value(item);
		return messages.stream().map(m -> execute(commands, key, m));
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private RedisFuture<Object> execute(RedisAsyncCommands<K, V> commands, K key, StreamMessage<K, V> message) {
		Map<K, V> body = message.getBody();
		if (CollectionUtils.isEmpty(body)) {
			return null;
		}
		XAddArgs args = argsFunction.apply(message);
		return (RedisFuture) commands.xadd(key, args, body);
	}

}
