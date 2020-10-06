package org.springframework.batch.item.redis;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.springframework.batch.item.redis.support.AbstractKeyValueItemWriter;
import org.springframework.batch.item.redis.support.KeyValue;
import org.springframework.batch.item.redis.support.RedisConnectionBuilder;

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
import lombok.Setter;
import lombok.experimental.Accessors;

public class RedisKeyValueItemWriter<K, V> extends AbstractKeyValueItemWriter<K, V, KeyValue<K>> {

	@Override
	@SuppressWarnings("unchecked")
	protected void doWrite(BaseRedisAsyncCommands<K, V> commands, List<RedisFuture<?>> futures, KeyValue<K> item) {
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
			futures.add(((RedisHashAsyncCommands<K, V>) commands).hmset(item.getKey(), (Map<K, V>) item.getValue()));
			break;
		case STREAM:
			Collection<StreamMessage<K, V>> messages = (Collection<StreamMessage<K, V>>) item.getValue();
			for (StreamMessage<K, V> message : messages) {
				futures.add(((RedisStreamAsyncCommands<K, V>) commands).xadd(item.getKey(),
						new XAddArgs().id(message.getId()), message.getBody()));
			}
			break;
		case NONE:
			break;
		}
	}

	@Override
	@SuppressWarnings("unchecked")
	protected void doWrite(BaseRedisAsyncCommands<K, V> commands, List<RedisFuture<?>> futures, KeyValue<K> item,
			long ttl) {
		doWrite(commands, futures, item);
		futures.add(((RedisKeyAsyncCommands<K, V>) commands).expire(item.getKey(), item.getTtl()));
	}

	public static RedisKeyValueItemWriterBuilder builder() {
		return new RedisKeyValueItemWriterBuilder();
	}

	@Setter
	@Accessors(fluent = true)
	public static class RedisKeyValueItemWriterBuilder extends RedisConnectionBuilder<RedisKeyValueItemWriterBuilder> {

		public RedisKeyValueItemWriter<String, String> build() {
			RedisKeyValueItemWriter<String, String> writer = new RedisKeyValueItemWriter<>();
			writer.setPool(pool());
			writer.setCommands(async());
			writer.setCommandTimeout(timeout());
			return writer;
		}

	}
}
