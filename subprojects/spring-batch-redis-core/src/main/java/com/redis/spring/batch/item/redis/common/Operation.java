package com.redis.spring.batch.item.redis.common;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

import com.redis.lettucemod.search.Suggestion;
import com.redis.lettucemod.timeseries.Sample;
import com.redis.spring.batch.item.redis.writer.Del;
import com.redis.spring.batch.item.redis.writer.Expire;
import com.redis.spring.batch.item.redis.writer.ExpireAt;
import com.redis.spring.batch.item.redis.writer.Geoadd;
import com.redis.spring.batch.item.redis.writer.Hset;
import com.redis.spring.batch.item.redis.writer.JsonDel;
import com.redis.spring.batch.item.redis.writer.JsonSet;
import com.redis.spring.batch.item.redis.writer.Lpush;
import com.redis.spring.batch.item.redis.writer.Noop;
import com.redis.spring.batch.item.redis.writer.Rpush;
import com.redis.spring.batch.item.redis.writer.Sadd;
import com.redis.spring.batch.item.redis.writer.Set;
import com.redis.spring.batch.item.redis.writer.Sugadd;
import com.redis.spring.batch.item.redis.writer.TsAdd;
import com.redis.spring.batch.item.redis.writer.Xadd;
import com.redis.spring.batch.item.redis.writer.Zadd;

import io.lettuce.core.GeoValue;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.ScoredValue;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;

public interface Operation<K, V, I, O> {

	void execute(BaseRedisAsyncCommands<K, V> commands, Iterable<? extends I> inputs, List<RedisFuture<O>> outputs);

	static <K, V, T> Del<K, V, T> del(Function<T, K> key) {
		return new Del<>(key);
	}

	static <K, V, T> Expire<K, V, T> expire(Function<T, K> key) {
		return new Expire<>(key);
	}

	static <K, V, T> ExpireAt<K, V, T> expireAt(Function<T, K> key) {
		return new ExpireAt<>(key);
	}

	static <K, V, T> Geoadd<K, V, T> geoadd(K key, Function<T, GeoValue<V>> value) {
		return new Geoadd<>(k -> key, value);
	}

	static <K, V, T> Geoadd<K, V, T> geoadd(Function<T, K> key, Function<T, GeoValue<V>> value) {
		return new Geoadd<>(key, value);
	}

	static <K, V, T> Hset<K, V, T> hset(K key, Function<T, Map<K, V>> value) {
		return new Hset<>(k -> key, value);
	}

	static <K, V, T> Hset<K, V, T> hset(Function<T, K> key, Function<T, Map<K, V>> value) {
		return new Hset<>(key, value);
	}

	static <K, V, T> JsonDel<K, V, T> jsonDel(Function<T, K> key) {
		return new JsonDel<>(key);
	}

	static <K, V, T> JsonSet<K, V, T> jsonSet(Function<T, K> key, Function<T, V> value) {
		return new JsonSet<>(key, value);
	}

	static <K, V, T> Lpush<K, V, T> lpush(K key, Function<T, V> value) {
		return new Lpush<>(k -> key, value);
	}

	static <K, V, T> Lpush<K, V, T> lpush(Function<T, K> key, Function<T, V> value) {
		return new Lpush<>(key, value);
	}

	static <K, V, T> Noop<K, V, T> noop() {
		return new Noop<>();
	}

	static <K, V, T> Rpush<K, V, T> rpush(K key, Function<T, V> value) {
		return new Rpush<>(k -> key, value);
	}

	static <K, V, T> Rpush<K, V, T> rpush(Function<T, K> key, Function<T, V> value) {
		return new Rpush<>(key, value);
	}

	static <K, V, T> Sadd<K, V, T> sadd(K key, Function<T, V> value) {
		return new Sadd<>(k -> key, value);
	}

	static <K, V, T> Sadd<K, V, T> sadd(Function<T, K> key, Function<T, V> value) {
		return new Sadd<>(key, value);
	}

	static <K, V, T> Set<K, V, T> set(Function<T, K> key, Function<T, V> value) {
		return new Set<>(key, value);
	}

	static <K, V, T> Sugadd<K, V, T> sugadd(K key, Function<T, Suggestion<V>> value) {
		return new Sugadd<>(k -> key, value);
	}

	static <K, V, T> Sugadd<K, V, T> sugadd(Function<T, K> key, Function<T, Suggestion<V>> value) {
		return new Sugadd<>(key, value);
	}

	static <K, V, T> TsAdd<K, V, T> tsAdd(K key, Function<T, Sample> value) {
		return new TsAdd<>(k -> key, value);
	}

	static <K, V, T> TsAdd<K, V, T> tsAdd(Function<T, K> key, Function<T, Sample> sample) {
		return new TsAdd<>(key, sample);
	}

	static <K, V, T> Xadd<K, V, T> xadd(K key, Function<T, Map<K, V>> body) {
		return new Xadd<>(k -> key, body);
	}

	static <K, V, T> Xadd<K, V, T> xadd(Function<T, K> key, Function<T, Map<K, V>> body) {
		return new Xadd<>(key, body);
	}

	static <K, V, T> Zadd<K, V, T> zadd(K key, Function<T, ScoredValue<V>> value) {
		return new Zadd<>(k -> key, value);
	}

	static <K, V, T> Zadd<K, V, T> zadd(Function<T, K> key, Function<T, ScoredValue<V>> value) {
		return new Zadd<>(key, value);
	}
}
