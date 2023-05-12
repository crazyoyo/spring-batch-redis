package com.redis.spring.batch.writer.operation;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.springframework.util.Assert;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.XAddArgs;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisStreamAsyncCommands;

public class Xadd<K, V, T> extends AbstractOperation<K, V, T> {

	private final Function<T, XAddArgs> args;
	private final Function<T, Map<K, V>> body;

	public Xadd(Function<T, K> key, Function<T, Map<K, V>> body) {
		this(key, body, nullArgs());
	}

	public Xadd(Function<T, K> key, Function<T, Map<K, V>> body, Function<T, XAddArgs> args) {
		super(key);
		Assert.notNull(body, "A body function is required");
		Assert.notNull(args, "Args function is required");
		this.body = body;
		this.args = args;
	}

	@Override
	@SuppressWarnings("unchecked")
	protected void execute(BaseRedisAsyncCommands<K, V> commands, List<RedisFuture<?>> futures, T item, K key) {
		Map<K, V> map = body.apply(item);
		if (map.isEmpty()) {
			return;
		}
		futures.add(((RedisStreamAsyncCommands<K, V>) commands).xadd(key, args.apply(item), map));
	}

	public static <T> Function<T, XAddArgs> nullArgs() {
		return m -> null;
	}

	public static <K, V> Function<StreamMessage<K, V>, XAddArgs> idArgs() {
		return m -> new XAddArgs().id(m.getId());
	}

}
