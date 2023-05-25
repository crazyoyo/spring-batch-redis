package com.redis.spring.batch.writer.operation;

import java.util.function.Function;

import com.redis.spring.batch.common.Operation;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisScriptingAsyncCommands;

public class Eval<K, V, T> implements Operation<K, V, T, Object> {

	private final String sha;
	private final ScriptOutputType output;
	private final Function<T, K[]> keys;
	private final Function<T, V[]> args;

	public Eval(String sha, ScriptOutputType output, Function<T, K[]> keys, Function<T, V[]> args) {
		this.sha = sha;
		this.output = output;
		this.keys = keys;
		this.args = args;
	}

	@SuppressWarnings("unchecked")
	@Override
	public RedisFuture<Object> execute(BaseRedisAsyncCommands<K, V> commands, T item) {
		return ((RedisScriptingAsyncCommands<K, V>) commands).evalsha(sha, output, keys.apply(item), args.apply(item));
	}

}
