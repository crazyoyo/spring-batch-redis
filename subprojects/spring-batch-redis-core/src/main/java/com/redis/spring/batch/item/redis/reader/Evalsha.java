package com.redis.spring.batch.item.redis.reader;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.redis.spring.batch.item.redis.common.BatchUtils;
import com.redis.spring.batch.item.redis.common.Operation;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.codec.RedisCodec;

@SuppressWarnings("unchecked")
public class Evalsha<K, V, I> implements Operation<K, V, I, List<Object>> {

	private static final Object[] EMPTY_ARRAY = new Object[0];

	private final Function<I, K> keyFunction;
	private final Function<String, V> stringValueFunction;

	private Function<I, V[]> argsFunction = t -> (V[]) EMPTY_ARRAY;
	private String digest;

	public Evalsha(RedisCodec<K, V> codec, Function<I, K> key) {
		this.stringValueFunction = BatchUtils.stringValueFunction(codec);
		this.keyFunction = key;
	}

	@Override
	public List<RedisFuture<List<Object>>> execute(RedisAsyncCommands<K, V> commands, List<? extends I> items) {
		return items.stream().map(t -> execute(commands, t)).collect(Collectors.toList());
	}

	public RedisFuture<List<Object>> execute(RedisAsyncCommands<K, V> commands, I item) {
		K[] keys = (K[]) new Object[] { keyFunction.apply(item) };
		V[] args = argsFunction.apply(item);
		return commands.evalsha(digest, ScriptOutputType.MULTI, keys, args);
	}

	public void setArgsFunction(Function<I, V[]> function) {
		this.argsFunction = function;
	}

	public void setArgs(Object... args) {
		V[] encodedArgs = (V[]) new Object[args.length];
		for (int index = 0; index < args.length; index++) {
			encodedArgs[index] = stringValueFunction.apply(String.valueOf(args[index]));
		}
		this.argsFunction = t -> encodedArgs;
	}

	public String getDigest() {
		return digest;
	}

	public void setDigest(String digest) {
		this.digest = digest;
	}
}
