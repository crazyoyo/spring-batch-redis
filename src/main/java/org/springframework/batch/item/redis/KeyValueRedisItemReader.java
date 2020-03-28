package org.springframework.batch.item.redis;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.springframework.batch.item.redis.KeyValueRedisItemReader.KeyValue;
import org.springframework.batch.item.redis.support.AbstractRedisItemReader;
import org.springframework.dao.DataAccessException;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.DataType;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.data.redis.serializer.RedisSerializer;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

public class KeyValueRedisItemReader<K, V> extends AbstractRedisItemReader<K, V, KeyValue<K>> {

	@NoArgsConstructor
	@AllArgsConstructor
	public static @Data class KeyValue<K> {
		private K key;
		private DataType type;
		private Object value;
	}

	@Builder
	protected KeyValueRedisItemReader(int currentItemCount, Integer maxItemCount, Boolean saveState,
			RedisTemplate<K, V> redisTemplate, ScanOptions scanOptions, int batchSize) {
		super(currentItemCount, maxItemCount, saveState, redisTemplate, scanOptions, batchSize);
	}

	@SuppressWarnings("unchecked")
	@Override
	protected List<KeyValue<K>> getValues(List<byte[]> keys) {
		List<Object> types = redisTemplate.executePipelined(new RedisCallback<Object>() {
			@Override
			public Object doInRedis(RedisConnection connection) throws DataAccessException {
				for (int index = 0; index < keys.size(); index++) {
					connection.type(keys.get(index));
				}
				return null;
			}
		});
		List<Object> redisValues = redisTemplate.executePipelined(new RedisCallback<Object>() {
			@Override
			public Object doInRedis(RedisConnection connection) throws DataAccessException {
				for (int index = 0; index < keys.size(); index++) {
					byte[] key = keys.get(index);
					DataType type = (DataType) types.get(index);
					switch (type) {
					case STRING:
						connection.get(key);
						break;
					case LIST:
						connection.lRange(key, 0, -1);
						break;
					case SET:
						connection.sMembers(key);
						break;
					case ZSET:
						connection.zRangeWithScores(key, 0, -1);
						break;
					case HASH:
						connection.hGetAll(key);
						break;
					case STREAM:
						connection.xRange(key, Range.closed("-", "+"));
						break;
					case NONE:
						break;
					}
				}
				return null;
			}
		});
		RedisSerializer<K> keySerializer = (RedisSerializer<K>) redisTemplate.getKeySerializer();
		List<KeyValue<K>> values = new ArrayList<>(keys.size());
		for (int index = 0; index < keys.size(); index++) {
			K key = keySerializer.deserialize(keys.get(index));
			DataType type = (DataType) types.get(index);
			Object value;
			if (type == DataType.STREAM) {
				List<MapRecord<byte[], byte[], byte[]>> messages = (List<MapRecord<byte[], byte[], byte[]>>) redisValues
						.get(index);
				value = messages.stream().map(r -> MapRecord
						.create(keySerializer.deserialize(r.getStream()), deserialize(r.getValue())).withId(r.getId()))
						.collect(Collectors.toList());
			} else {
				value = redisValues.get(index);
			}
			values.add(new KeyValue<K>(key, type, value));
		}
		return values;

	}

	@SuppressWarnings("unchecked")
	private Map<K, V> deserialize(Map<byte[], byte[]> value) {
		RedisSerializer<K> keySerializer = (RedisSerializer<K>) redisTemplate.getKeySerializer();
		RedisSerializer<V> valueSerializer = (RedisSerializer<V>) redisTemplate.getValueSerializer();
		Map<K, V> map = new HashMap<>();
		value.forEach((k, v) -> map.put(keySerializer.deserialize(k), valueSerializer.deserialize(v)));
		return map;
	}

}
