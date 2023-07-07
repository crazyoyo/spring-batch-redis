package com.redis.spring.batch.writer.operation;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Future;
import java.util.function.Function;

import com.redis.spring.batch.common.CompositeFuture;
import com.redis.spring.batch.common.NoOpFuture;
import com.redis.spring.batch.common.Operation;

import io.lettuce.core.StreamMessage;
import io.lettuce.core.XAddArgs;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;

public class XAddAll<K, V, T> implements Operation<K, V, T, List<String>> {

	private final Function<T, Collection<StreamMessage<K, V>>> messages;
	private final Xadd<K, V, StreamMessage<K, V>> xadd;

	public XAddAll(Function<T, Collection<StreamMessage<K, V>>> messages,
			Function<StreamMessage<K, V>, XAddArgs> args) {
		this.messages = messages;
		this.xadd = new Xadd<>(StreamMessage::getStream, StreamMessage::getBody, args);
	}

	@SuppressWarnings("unchecked")
	@Override
	public Future<List<String>> execute(BaseRedisAsyncCommands<K, V> commands, T item) {
		Collection<StreamMessage<K, V>> collection = messages.apply(item);
		if (collection.isEmpty()) {
			return NoOpFuture.instance();
		}
		List<Future<String>> futures = new ArrayList<>();
		for (StreamMessage<K, V> message : collection) {
			futures.add(xadd.execute(commands, message));
		}
		return new CompositeFuture<>(futures);
	}

}