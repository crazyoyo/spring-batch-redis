package org.springframework.batch.item.redis;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.springframework.batch.item.redis.support.AbstractKeyValue;
import org.springframework.batch.item.redis.support.AbstractRedisItemWriter;
import org.springframework.batch.item.redis.support.KeyValue;
import org.springframework.batch.item.redis.support.RedisItemWriterBuilder;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.ScoredValue;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.XAddArgs;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisHashAsyncCommands;
import io.lettuce.core.api.async.RedisKeyAsyncCommands;
import io.lettuce.core.api.async.RedisListAsyncCommands;
import io.lettuce.core.api.async.RedisSetAsyncCommands;
import io.lettuce.core.api.async.RedisSortedSetAsyncCommands;
import io.lettuce.core.api.async.RedisStreamAsyncCommands;
import io.lettuce.core.api.async.RedisStringAsyncCommands;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;

public class RedisKeyValueItemWriter<K, V> extends AbstractRedisItemWriter<K, V, KeyValue<K>> {

	@SuppressWarnings({ "unchecked", "incomplete-switch" })
	@Override
	protected List<RedisFuture<?>> write(BaseRedisAsyncCommands<K, V> commands, List<? extends KeyValue<K>> items) {
		List<RedisFuture<?>> futures = new ArrayList<>();
		for (KeyValue<K> item : items) {
			if (item.getValue() == null || item.getTtl() == AbstractKeyValue.TTL_NOT_EXISTS) {
				futures.add(((RedisKeyAsyncCommands<K, V>) commands).del(item.getKey()));
				continue;
			}
			if (item.getValue() != null) {
				switch (item.getType()) {
				case STRING:
					futures.add(((RedisStringAsyncCommands<K, V>) commands).set(item.getKey(), (V) item.getValue()));
					break;
				case LIST:
					futures.add(((RedisListAsyncCommands<K, V>) commands).lpush(item.getKey(),
							(V[]) ((Collection<V>) item.getValue()).toArray()));
					break;
				case SET:
					futures.add(((RedisSetAsyncCommands<K, V>) commands).sadd(item.getKey(),
							(V[]) ((Collection<V>) item.getValue()).toArray()));
					break;
				case ZSET:
					Collection<ScoredValue<V>> scoredValues = (Collection<ScoredValue<V>>) item.getValue();
					ScoredValue<V>[] scoredValuesArray = (ScoredValue<V>[]) scoredValues
							.toArray(new ScoredValue[scoredValues.size()]);
					futures.add(((RedisSortedSetAsyncCommands<K, V>) commands).zadd(item.getKey(), scoredValuesArray));
					break;
				case HASH:
					futures.add(((RedisHashAsyncCommands<K, V>) commands).hmset(item.getKey(),
							(Map<K, V>) item.getValue()));
					break;
				case STREAM:
					Collection<StreamMessage<K, V>> messages = (Collection<StreamMessage<K, V>>) item.getValue();
					for (StreamMessage<K, V> message : messages) {
						futures.add(((RedisStreamAsyncCommands<K, V>) commands).xadd(item.getKey(),
								new XAddArgs().id(message.getId()), message.getBody()));
					}
					break;
				}
			}
			if (item.getTtl() > 0) {
				futures.add(((RedisKeyAsyncCommands<K, V>) commands).expire(item.getKey(), item.getTtl()));
			}
		}
		return futures;
	}

	@Override
	protected RedisFuture<?> write(BaseRedisAsyncCommands<K, V> commands, KeyValue<K> item) {
		// not used
		return null;
	}

	public static RedisKeyValueItemWriterBuilder<String, String> builder() {
		return new RedisKeyValueItemWriterBuilder<>(StringCodec.UTF8);
	}

	public static class RedisKeyValueItemWriterBuilder<K, V>
			extends RedisItemWriterBuilder<K, V, RedisKeyValueItemWriterBuilder<K, V>> {

		public RedisKeyValueItemWriterBuilder(RedisCodec<K, V> codec) {
			super(codec);
		}

		public RedisKeyValueItemWriter<K, V> build() {
			return configure(new RedisKeyValueItemWriter<>());
		}

	}
}
