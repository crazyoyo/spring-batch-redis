package com.redis.spring.batch.reader;

import java.util.List;
import java.util.function.Function;

import com.redis.spring.batch.Operation;
import com.redis.spring.batch.util.BatchUtils;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisScriptingAsyncCommands;
import io.lettuce.core.codec.RedisCodec;

@SuppressWarnings("unchecked")
public class Evalsha<K, V, I> implements Operation<K, V, I, List<Object>> {

	private static final Object[] EMPTY_ARRAY = new Object[0];

	private final String digest;
	private final Function<I, K> keyFunction;
	private final Function<String, V> stringValueFunction;

	private Function<I, V[]> argsFunction = t -> (V[]) EMPTY_ARRAY;

	public Evalsha(String digest, RedisCodec<K, V> codec, Function<I, K> key) {
		this.digest = digest;
		this.stringValueFunction = BatchUtils.stringValueFunction(codec);
		this.keyFunction = key;
	}

	@Override
	public void execute(BaseRedisAsyncCommands<K, V> commands, Iterable<? extends I> inputs,
			List<RedisFuture<List<Object>>> outputs) {
		RedisScriptingAsyncCommands<K, V> scriptingCommands = (RedisScriptingAsyncCommands<K, V>) commands;
		for (I item : inputs) {
			K[] keys = (K[]) new Object[] { keyFunction.apply(item) };
			V[] args = argsFunction.apply(item);
			outputs.add(scriptingCommands.evalsha(digest, ScriptOutputType.MULTI, keys, args));
		}
	}

	public void setArgs(Function<I, V[]> function) {
		this.argsFunction = function;
	}

	public void setArgs(Object... args) {
		V[] encodedArgs = (V[]) new Object[args.length];
		for (int index = 0; index < args.length; index++) {
			encodedArgs[index] = stringValueFunction.apply(String.valueOf(args[index]));
		}
		this.argsFunction = t -> encodedArgs;
	}

}
