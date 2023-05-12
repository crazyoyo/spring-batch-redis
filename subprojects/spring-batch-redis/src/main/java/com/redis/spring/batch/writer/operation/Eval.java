package com.redis.spring.batch.writer.operation;

import java.util.List;
import java.util.function.Function;

import org.springframework.util.Assert;

import com.redis.spring.batch.writer.WriteOperation;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisScriptingAsyncCommands;

public class Eval<K, V, T> implements WriteOperation<K, V, T> {

	private final String sha;
	private final ScriptOutputType output;
	private final Function<T, K[]> keys;
	private final Function<T, V[]> args;

	public Eval(String sha, ScriptOutputType output, Function<T, K[]> keys, Function<T, V[]> args) {
		Assert.notNull(sha, "A SHA digest is required");
		Assert.notNull(output, "A script output type is required");
		Assert.notNull(keys, "Keys function is required");
		Assert.notNull(args, "Args function is required");
		this.sha = sha;
		this.output = output;
		this.keys = keys;
		this.args = args;
	}

	@SuppressWarnings("unchecked")
	@Override
	public void execute(BaseRedisAsyncCommands<K, V> commands, List<RedisFuture<?>> futures, T item) {
		futures.add(((RedisScriptingAsyncCommands<K, V>) commands).evalsha(sha, output, keys.apply(item),
				args.apply(item)));
	}

}
